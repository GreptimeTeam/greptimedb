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

use async_trait::async_trait;
use snafu::{OptionExt, ResultExt};

use crate::error::{FromUtf8Snafu, InvalidConfigSnafu, Result};
use crate::user_provider::{
    UserInfoMap, authenticate_with_credential, load_credential_from_file, parse_credential_line,
};
use crate::{Identity, Password, UserInfoRef, UserProvider};

pub(crate) const STATIC_USER_PROVIDER: &str = "static_user_provider";

pub struct StaticUserProvider {
    users: UserInfoMap,
}

impl StaticUserProvider {
    pub(crate) fn new(value: &str) -> Result<Self> {
        let (mode, content) = value.split_once(':').context(InvalidConfigSnafu {
            value: value.to_string(),
            msg: "StaticUserProviderOption must be in format `<option>:<value>`",
        })?;
        match mode {
            "file" => {
                let users = load_credential_from_file(content)?;
                Ok(StaticUserProvider { users })
            }
            "cmd" => content
                .split(',')
                .map(|kv| {
                   parse_credential_line(kv).context(InvalidConfigSnafu {
                        value: kv.to_string(),
                        msg: "StaticUserProviderOption cmd values must be in format `user=pwd[,user=pwd]`",
                    })
                })
                .collect::<Result<UserInfoMap>>()
                .map(|users| StaticUserProvider { users }),
            _ => InvalidConfigSnafu {
                value: mode.to_string(),
                msg: "StaticUserProviderOption must be in format `file:<path>` or `cmd:<values>`",
            }
                .fail(),
        }
    }

    /// Return a random username/password pair
    /// This is useful for invoking from other components in the cluster
    pub fn get_one_user_pwd(&self) -> Result<(String, String)> {
        let kv = self.users.iter().next().context(InvalidConfigSnafu {
            value: "",
            msg: "Expect at least one pair of username and password",
        })?;
        let username = kv.0;
        let pwd = String::from_utf8(kv.1.0.clone()).context(FromUtf8Snafu)?;
        Ok((username.clone(), pwd))
    }
}

#[async_trait]
impl UserProvider for StaticUserProvider {
    fn name(&self) -> &str {
        STATIC_USER_PROVIDER
    }

    async fn authenticate(&self, id: Identity<'_>, pwd: Password<'_>) -> Result<UserInfoRef> {
        authenticate_with_credential(&self.users, id, pwd)
    }

    async fn authorize(
        &self,
        _catalog: &str,
        _schema: &str,
        _user_info: &UserInfoRef,
    ) -> Result<()> {
        // default allow all
        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    use std::fs::File;
    use std::io::{LineWriter, Write};

    use common_test_util::temp_dir::create_temp_dir;

    use crate::UserProvider;
    use crate::user_info::DefaultUserInfo;
    use crate::user_provider::static_user_provider::StaticUserProvider;
    use crate::user_provider::{Identity, Password};

    async fn test_authenticate(provider: &dyn UserProvider, username: &str, password: &str) {
        let re = provider
            .authenticate(
                Identity::UserId(username, None),
                Password::PlainText(password.to_string().into()),
            )
            .await;
        let _ = re.unwrap();
    }

    #[tokio::test]
    async fn test_authorize() {
        let user_info = DefaultUserInfo::with_name("root");
        let provider = StaticUserProvider::new("cmd:root=123456,admin=654321").unwrap();
        provider
            .authorize("catalog", "schema", &user_info)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_inline_provider() {
        let provider = StaticUserProvider::new("cmd:root=123456,admin=654321").unwrap();
        test_authenticate(&provider, "root", "123456").await;
        test_authenticate(&provider, "admin", "654321").await;
    }

    #[tokio::test]
    async fn test_file_provider() {
        let dir = create_temp_dir("test_file_provider");
        let file_path = format!("{}/test_file_provider", dir.path().to_str().unwrap());
        {
            // write a tmp file
            let file = File::create(&file_path);
            let file = file.unwrap();
            let mut lw = LineWriter::new(file);
            assert!(
                lw.write_all(
                    b"root=123456
admin=654321",
                )
                .is_ok()
            );
            lw.flush().unwrap();
        }

        let param = format!("file:{file_path}");
        let provider = StaticUserProvider::new(param.as_str()).unwrap();
        test_authenticate(&provider, "root", "123456").await;
        test_authenticate(&provider, "admin", "654321").await;
    }
}
