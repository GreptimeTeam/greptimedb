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
    Error, Identity, InvalidConfigSnafu, Password, UserInfo, UserNotExistSnafu, UserProvider,
    WrongPwdSnafu,
};
use crate::error::InternalIoSnafu;

pub enum MemUserProviderOption {
    FilePath(String),
    Inline(HashMap<String, Vec<u8>>),
}

impl TryFrom<&str> for MemUserProviderOption {
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

                Ok(MemUserProviderOption::FilePath(content.to_string()))
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
                .map(MemUserProviderOption::Inline),
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

impl MemUserProvider {
    pub fn new(opts: MemUserProviderOption) -> Self {
        let users = match opts {
            MemUserProviderOption::FilePath(path) => {
                // todo(shuiyisong): figure out how to process error
                let file = File::open(&path).context(InternalIoSnafu).unwrap();
                io::BufReader::new(file)
                    .lines()
                    .filter_map(|line| line.ok())
                    .filter_map(|line| {
                        if let Some((k, v)) = line.split_once('=') {
                            Some((k.to_string(), v.as_bytes().to_vec()))
                        } else {
                            None
                        }
                    })
                    .collect::<HashMap<String, Vec<u8>>>()
            }
            MemUserProviderOption::Inline(users) => users,
        };
        Self { users }
    }

    pub fn add_user(&mut self, name: String, password: Vec<u8>) {
        self.users.insert(name, password);
    }
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
                            return if save_pwd == pwd {
                                Ok(UserInfo {
                                    username: username.to_string(),
                                })
                            } else {
                                UserNotExistSnafu {}.fail()
                            }
                        }
                        Password::MysqlNativePwd(auth_data, salt) => {
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
                                WrongPwdSnafu {}.fail()
                            };
                        }
                        _ => unimplemented!(),
                    }
                } else {
                    UserNotExistSnafu {}.fail()
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
