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

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::Path;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use secrecy::ExposeSecret;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    IllegalParamSnafu, InvalidConfigSnafu, IoSnafu, Result, UnsupportedPasswordTypeSnafu,
    UserNotFoundSnafu, UserPasswordMismatchSnafu,
};
use crate::user_info::DefaultUserInfo;
use crate::{auth_mysql, Identity, Password, UserInfoRef, UserProvider};

pub(crate) const STATIC_USER_PROVIDER: &str = "static_user_provider";

pub(crate) struct StaticUserProvider {
    ident: String,
    users: AtomicPtr<HashMap<String, Vec<u8>>>,
}

impl StaticUserProvider {
    pub(crate) fn new(ident: String) -> Result<Self> {
        let users = Box::into_raw(Box::new(HashMap::new()));
        let this = StaticUserProvider {
            ident,
            users: AtomicPtr::new(users),
        };
        this.reload()?;
        Ok(this)
    }
}

#[async_trait]
impl UserProvider for StaticUserProvider {
    fn name(&self) -> &str {
        STATIC_USER_PROVIDER
    }

    fn reload(&self) -> Result<()> {
        let value = self.ident.as_str();
        let (mode, content) = value.split_once(':').context(InvalidConfigSnafu {
            value: value.to_string(),
            msg: "StaticUserProviderOption must be in format `<option>:<value>`",
        })?;

        ensure!(
            matches!(mode, "file" | "cmd"),
            InvalidConfigSnafu {
                value: mode.to_string(),
                msg: "StaticUserProviderOption must be in format `file:<path>` or `cmd:<values>`",
            }
        );

        match mode {
            "file" => {
                // check valid path
                let path = Path::new(content);
                ensure!(
                    path.exists() && path.is_file(),
                    InvalidConfigSnafu {
                        value: content.to_string(),
                        msg: "StaticUserProviderOption file must be a valid file path",
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

                ensure!(!credential.is_empty(), InvalidConfigSnafu {
                    value: content.to_string(),
                    msg: "StaticUserProviderOption file must contains at least one valid credential",
                });

                let credential = Box::into_raw(Box::new(credential));
                let users = self.users.swap(credential, Ordering::SeqCst);
                unsafe { Box::from_raw(users) };
            }
            "cmd" => {
                // cmd is immutable - need not reload
                if unsafe { &*self.users.load(Ordering::SeqCst) }.is_empty() {
                    let credential = content
                        .split(',')
                        .map(|kv| {
                            let (k, v) = kv.split_once('=').context(InvalidConfigSnafu {
                                value: kv.to_string(),
                                msg: "StaticUserProviderOption cmd values must be in format `user=pwd[,user=pwd]`",
                            })?;
                            Ok((k.to_string(), v.as_bytes().to_vec()))
                        })
                        .collect::<Result<HashMap<String, Vec<u8>>>>()?;
                    let credential = Box::into_raw(Box::new(credential));
                    let users = self.users.swap(credential, Ordering::SeqCst);
                    unsafe { Box::from_raw(users) };
                }
            }
            mode => unreachable!("invalid mode {} must be filtered above", mode),
        };

        Ok(())
    }

    async fn authenticate(
        &self,
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
                let users = unsafe { &*self.users.load(Ordering::SeqCst) };
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
                        return if save_pwd == pwd.expose_secret().as_bytes() {
                            Ok(DefaultUserInfo::with_name(username))
                        } else {
                            UserPasswordMismatchSnafu {
                                username: username.to_string(),
                            }
                            .fail()
                        };
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

    use crate::user_info::DefaultUserInfo;
    use crate::user_provider::static_user_provider::StaticUserProvider;
    use crate::user_provider::{Identity, Password};
    use crate::UserProvider;

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
        let provider = StaticUserProvider::new("cmd:root=123456,admin=654321".into()).unwrap();
        provider
            .authorize("catalog", "schema", &user_info)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_inline_provider() {
        let provider = StaticUserProvider::new("cmd:root=123456,admin=654321".into()).unwrap();
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
            assert!(lw
                .write_all(
                    b"root=123456
admin=654321",
                )
                .is_ok());
            lw.flush().unwrap();
        }

        let param = format!("file:{file_path}");
        let provider = StaticUserProvider::new(param).unwrap();
        test_authenticate(&provider, "root", "123456").await;
        test_authenticate(&provider, "admin", "654321").await;
    }
}
