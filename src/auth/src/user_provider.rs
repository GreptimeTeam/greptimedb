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

pub(crate) mod static_user_provider;
pub(crate) mod watch_file_user_provider;

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::Path;

use common_base::secrets::ExposeSecret;
use snafu::{ensure, OptionExt, ResultExt};

use crate::common::{Identity, Password};
use crate::error::{
    IllegalParamSnafu, InvalidConfigSnafu, IoSnafu, Result, UnsupportedPasswordTypeSnafu,
    UserNotFoundSnafu, UserPasswordMismatchSnafu,
};
use crate::user_info::DefaultUserInfo;
use crate::{auth_mysql, UserInfoRef};

#[async_trait::async_trait]
pub trait UserProvider: Send + Sync {
    fn name(&self) -> &str;

    /// Checks whether a user is valid and allowed to access the database.
    async fn authenticate(&self, id: Identity<'_>, password: Password<'_>) -> Result<UserInfoRef>;

    /// Checks whether a connection request
    /// from a certain user to a certain catalog/schema is legal.
    /// This method should be called after [authenticate()](UserProvider::authenticate()).
    async fn authorize(&self, catalog: &str, schema: &str, user_info: &UserInfoRef) -> Result<()>;

    /// Combination of [authenticate()](UserProvider::authenticate()) and [authorize()](UserProvider::authorize()).
    /// In most cases it's preferred for both convenience and performance.
    async fn auth(
        &self,
        id: Identity<'_>,
        password: Password<'_>,
        catalog: &str,
        schema: &str,
    ) -> Result<UserInfoRef> {
        let user_info = self.authenticate(id, password).await?;
        self.authorize(catalog, schema, &user_info).await?;
        Ok(user_info)
    }

    /// Returns whether this user provider implementation is backed by an external system.
    fn external(&self) -> bool {
        false
    }
}

fn load_credential_from_file(filepath: &str) -> Result<Option<HashMap<String, Vec<u8>>>> {
    // check valid path
    let path = Path::new(filepath);
    if !path.exists() {
        return Ok(None);
    }

    ensure!(
        path.is_file(),
        InvalidConfigSnafu {
            value: filepath,
            msg: "UserProvider file must be a file",
        }
    );
    let file = File::open(path).context(IoSnafu)?;
    let credential = io::BufReader::new(file)
        .lines()
        .map_while(std::result::Result::ok)
        .filter_map(|line| {
            if let Some((k, v)) = line.split_once('=') {
                Some((k.to_string(), v.as_bytes().to_vec()))
            } else {
                None
            }
        })
        .collect::<HashMap<String, Vec<u8>>>();

    ensure!(
        !credential.is_empty(),
        InvalidConfigSnafu {
            value: filepath,
            msg: "UserProvider's file must contains at least one valid credential",
        }
    );

    Ok(Some(credential))
}

fn authenticate_with_credential(
    users: &HashMap<String, Vec<u8>>,
    input_id: Identity<'_>,
    input_pwd: Password<'_>,
) -> Result<UserInfoRef> {
    match input_id {
        Identity::UserId(username, _) => {
            ensure!(
                !username.is_empty(),
                IllegalParamSnafu {
                    msg: "blank username"
                }
            );
            let save_pwd = users.get(username).context(UserNotFoundSnafu {
                username: username.to_string(),
            })?;

            match input_pwd {
                Password::PlainText(pwd) => {
                    ensure!(
                        !pwd.expose_secret().is_empty(),
                        IllegalParamSnafu {
                            msg: "blank password"
                        }
                    );
                    if save_pwd == pwd.expose_secret().as_bytes() {
                        Ok(DefaultUserInfo::with_name(username))
                    } else {
                        UserPasswordMismatchSnafu {
                            username: username.to_string(),
                        }
                        .fail()
                    }
                }
                Password::MysqlNativePassword(auth_data, salt) => {
                    auth_mysql(auth_data, salt, username, save_pwd)
                        .map(|_| DefaultUserInfo::with_name(username))
                }
                Password::PgMD5(_, _) => UnsupportedPasswordTypeSnafu {
                    password_type: "pg_md5",
                }
                .fail(),
            }
        }
    }
}
