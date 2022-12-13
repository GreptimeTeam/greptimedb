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

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::Path;

use async_trait::async_trait;
use digest;
use digest::Digest;
use sha1::Sha1;
use snafu::{OptionExt, ResultExt};

use crate::auth::{
    Error, IOErrSnafu, Identity, InvalidConfigSnafu, Password, UserInfo, UserNotFoundSnafu,
    UserPasswordMismatchSnafu, UserProvider,
};

impl TryFrom<&str> for MemUserProvider {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let (mode, content) = value.split_once(':').context(InvalidConfigSnafu {
            value: value.to_string(),
            msg: "MemUserProviderOption must be in format `<option>:<value>`",
        })?;
        return match mode {
            "file" => {
                // check valid path
                let path = Path::new(content);
                if !path.exists() || !path.is_file() {
                    return InvalidConfigSnafu {
                        value: content.to_string(),
                        msg: "MemUserProviderOption file path must be a valid file",
                    }
                    .fail();
                }

                let file = File::open(path).context(IOErrSnafu)?;
                let credential = io::BufReader::new(file)
                    .lines()
                    .filter_map(|line| line.ok())
                    .filter_map(|line| {
                        if let Some((k, v)) = line.split_once('=') {
                            Some((k.to_string(), v.as_bytes().to_vec()))
                        } else {
                            None
                        }
                    })
                    .collect::<HashMap<String, Vec<u8>>>();
                if credential.is_empty() {
                    return InvalidConfigSnafu {
                        value: content.to_string(),
                        msg: "MemUserProviderOption file must contain at least one valid credential",
                    }
                    .fail();
                }

                Ok(MemUserProvider { users: credential, })
            }
            "inline" => content
                .split(',')
                .map(|kv| {
                    let (k, v) = kv.split_once('=').context(InvalidConfigSnafu {
                        value: kv.to_string(),
                        msg: "MemUserProviderOption inline values must be in format `user=pwd[,user=pwd]`",
                    })?;
                    Ok((k.to_string(), v.as_bytes().to_vec()))
                })
                .collect::<Result<HashMap<String, Vec<u8>>, Error>>()
                .map(|users| MemUserProvider { users }),
            _ => InvalidConfigSnafu {
                value: mode.to_string(),
                msg: "MemUserProviderOption must be in format `file:<path>` or `inline:<values>`",
            }
            .fail(),
        };
    }
}

pub struct MemUserProvider {
    users: HashMap<String, Vec<u8>>,
}

#[async_trait]
impl UserProvider for MemUserProvider {
    fn name(&self) -> &str {
        "mem_user_provider"
    }

    async fn auth(
        &self,
        input_id: Identity<'_>,
        input_pwd: Password<'_>,
    ) -> Result<UserInfo, Error> {
        match input_id {
            Identity::UserId(username, _) => {
                if let Some(save_pwd) = self.users.get(username) {
                    match input_pwd {
                        Password::PlainText(pwd) => {
                            return if save_pwd == pwd.as_bytes() {
                                Ok(UserInfo {
                                    username: username.to_string(),
                                })
                            } else {
                                UserNotFoundSnafu {}.fail()
                            }
                        }
                        Password::MysqlNativePassword(auth_data, salt) => {
                            // ref: https://github.com/mysql/mysql-server/blob/a246bad76b9271cb4333634e954040a970222e0a/sql/auth/password.cc#L62
                            let hash_stage_2 = double_sha1(save_pwd);
                            let tmp = sha1_two(salt, &hash_stage_2);
                            // xor auth_data and tmp
                            let mut xor_result = [0u8; 20];
                            for i in 0..20 {
                                xor_result[i] = auth_data[i] ^ tmp[i];
                            }
                            let candidate_stage_2 = sha1_one(&xor_result);
                            return if candidate_stage_2 == hash_stage_2 {
                                Ok(UserInfo {
                                    username: username.to_string(),
                                })
                            } else {
                                UserPasswordMismatchSnafu {}.fail()
                            };
                        }
                        _ => unimplemented!(),
                    }
                } else {
                    UserNotFoundSnafu {}.fail()
                }
            }
        }
    }
}

fn sha1_two(input_1: &[u8], input_2: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(input_1);
    hasher.update(input_2);
    hasher.finalize().to_vec()
}

fn sha1_one(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

fn double_sha1(data: &[u8]) -> Vec<u8> {
    sha1_one(&sha1_one(data))
}

#[cfg(test)]
pub mod test {
    use std::fs::File;
    use std::io::{LineWriter, Write};

    use crate::auth::user_provider::{double_sha1, sha1_one, sha1_two, MemUserProvider};
    use crate::auth::{Identity, Password, UserProvider};

    #[test]
    fn test_sha() {
        let sha_1_answer: Vec<u8> = vec![
            124, 74, 141, 9, 202, 55, 98, 175, 97, 229, 149, 32, 148, 61, 194, 100, 148, 248, 148,
            27,
        ];
        let sha_1 = sha1_one("123456".as_bytes());
        assert_eq!(sha_1, sha_1_answer);

        let double_sha1_answer: Vec<u8> = vec![
            107, 180, 131, 126, 183, 67, 41, 16, 94, 228, 86, 141, 218, 125, 198, 126, 210, 202,
            42, 217,
        ];
        let double_sha1 = double_sha1("123456".as_bytes());
        assert_eq!(double_sha1, double_sha1_answer);

        let sha1_2_answer: Vec<u8> = vec![
            132, 115, 215, 211, 99, 186, 164, 206, 168, 152, 217, 192, 117, 47, 240, 252, 142, 244,
            37, 204,
        ];
        let sha1_2 = sha1_two("123456".as_bytes(), "654321".as_bytes());
        assert_eq!(sha1_2, sha1_2_answer);
    }

    async fn test_auth(provider: &dyn UserProvider, username: &str, password: &str) {
        let re = provider
            .auth(
                Identity::UserId(username, None),
                Password::PlainText(password),
            )
            .await;
        assert!(re.is_ok());
    }

    #[tokio::test]
    async fn test_inline_provider() {
        let provider = MemUserProvider::try_from("inline:root=123456,admin=654321").unwrap();
        test_auth(&provider, "root", "123456").await;
        test_auth(&provider, "admin", "654321").await;
    }

    #[tokio::test]
    async fn test_file_provider() {
        {
            // write a tmp file
            let file = File::create("/tmp/test_file_provider");
            assert!(file.is_ok());
            let file = file.unwrap();
            let mut lw = LineWriter::new(file);
            assert!(lw
                .write_all(
                    b"root=123456
admin=654321",
                )
                .is_ok());
            assert!(lw.flush().is_ok());
        }

        let provider = MemUserProvider::try_from("file:/tmp/test_file_provider").unwrap();
        test_auth(&provider, "root", "123456").await;
        test_auth(&provider, "admin", "654321").await;

        // remove test file
        std::fs::remove_file("/tmp/test_file_provider").unwrap();
    }
}
