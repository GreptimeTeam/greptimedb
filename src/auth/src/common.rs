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

use digest::Digest;
use secrecy::SecretString;
use sha1::Sha1;
use snafu::{ensure, OptionExt};

use crate::error::{IllegalParamSnafu, InvalidConfigSnafu, Result, UserPasswordMismatchSnafu};
use crate::user_info::DefaultUserInfo;
use crate::user_provider::static_user_provider::{StaticUserProvider, STATIC_USER_PROVIDER};
use crate::{UserInfoRef, UserProviderRef};

pub(crate) const DEFAULT_USERNAME: &str = "greptime";

/// construct a [`UserInfo`](crate::user_info::UserInfo) impl with name
/// use default username `greptime` if None is provided
pub fn userinfo_by_name(username: Option<String>) -> UserInfoRef {
    DefaultUserInfo::with_name(username.unwrap_or_else(|| DEFAULT_USERNAME.to_string()))
}

pub fn user_provider_from_option(opt: &String) -> Result<UserProviderRef> {
    let (name, content) = opt.split_once(':').context(InvalidConfigSnafu {
        value: opt.to_string(),
        msg: "UserProviderOption must be in format `<option>:<value>`",
    })?;
    match name {
        STATIC_USER_PROVIDER => {
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
    PlainText(SecretString),
    MysqlNativePassword(HashedPassword<'a>, Salt<'a>),
    PgMD5(HashedPassword<'a>, Salt<'a>),
}

pub fn auth_mysql(
    auth_data: HashedPassword,
    salt: Salt,
    username: &str,
    save_pwd: &[u8],
) -> Result<()> {
    ensure!(
        auth_data.len() == 20,
        IllegalParamSnafu {
            msg: "Illegal mysql password length"
        }
    );
    // ref: https://github.com/mysql/mysql-server/blob/a246bad76b9271cb4333634e954040a970222e0a/sql/auth/password.cc#L62
    let hash_stage_2 = double_sha1(save_pwd);
    let tmp = sha1_two(salt, &hash_stage_2);
    // xor auth_data and tmp
    let mut xor_result = [0u8; 20];
    for i in 0..20 {
        xor_result[i] = auth_data[i] ^ tmp[i];
    }
    let candidate_stage_2 = sha1_one(&xor_result);
    if candidate_stage_2 == hash_stage_2 {
        Ok(())
    } else {
        UserPasswordMismatchSnafu {
            username: username.to_string(),
        }
        .fail()
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
mod tests {
    use super::*;

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
}
