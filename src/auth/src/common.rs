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

use common_base::secrets::SecretString;
use digest::Digest;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use sha1::Sha1;
use sha2::Sha256;
use snafu::{OptionExt, ensure};
use subtle::ConstantTimeEq;

use crate::error::{IllegalParamSnafu, InvalidConfigSnafu, Result, UserPasswordMismatchSnafu};
use crate::user_info::DefaultUserInfo;
use crate::user_provider::static_user_provider::{STATIC_USER_PROVIDER, StaticUserProvider};
use crate::user_provider::watch_file_user_provider::{
    WATCH_FILE_USER_PROVIDER, WatchFileUserProvider,
};
use crate::{UserInfoRef, UserProviderRef};

pub(crate) const DEFAULT_USERNAME: &str = "greptime";
pub const DEFAULT_PBKDF2_SHA256_ITERATIONS: u32 = 4096;
pub const PBKDF2_SHA256_HASH_LEN: usize = 32;
pub const MAX_PBKDF2_SHA256_ITERATIONS: u32 = 1_000_000;
pub const MAX_PBKDF2_SHA256_SALT_LEN: usize = 1024;
pub const PG_SCRAM_SHA256_KEY_LEN: usize = 32;

type HmacSha256 = Hmac<Sha256>;

/// construct a [`UserInfo`](crate::user_info::UserInfo) impl with name
/// use default username `greptime` if None is provided
pub fn userinfo_by_name(username: Option<String>) -> UserInfoRef {
    DefaultUserInfo::with_name(username.unwrap_or_else(|| DEFAULT_USERNAME.to_string()))
}

pub fn user_provider_from_option(opt: &str) -> Result<UserProviderRef> {
    let (name, content) = opt.split_once(':').with_context(|| InvalidConfigSnafu {
        value: opt.to_string(),
        msg: "UserProviderOption must be in format `<option>:<value>`",
    })?;
    match name {
        STATIC_USER_PROVIDER => {
            let provider =
                StaticUserProvider::new(content).map(|p| Arc::new(p) as UserProviderRef)?;
            Ok(provider)
        }
        WATCH_FILE_USER_PROVIDER => {
            WatchFileUserProvider::new(content).map(|p| Arc::new(p) as UserProviderRef)
        }
        _ => InvalidConfigSnafu {
            value: name.to_string(),
            msg: "Invalid UserProviderOption",
        }
        .fail(),
    }
}

pub fn static_user_provider_from_option(opt: &str) -> Result<StaticUserProvider> {
    let (name, content) = opt.split_once(':').with_context(|| InvalidConfigSnafu {
        value: opt.to_string(),
        msg: "UserProviderOption must be in format `<option>:<value>`",
    })?;
    match name {
        STATIC_USER_PROVIDER => {
            let provider = StaticUserProvider::new(content)?;
            Ok(provider)
        }
        _ => InvalidConfigSnafu {
            value: name.to_string(),
            msg: format!("Invalid UserProviderOption, expect only {STATIC_USER_PROVIDER}"),
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

impl Password<'_> {
    pub fn r#type(&self) -> &str {
        match self {
            Password::PlainText(_) => "plain_text",
            Password::MysqlNativePassword(_, _) => "mysql_native_password",
            Password::PgMD5(_, _) => "pg_md5",
        }
    }
}

pub fn auth_mysql(
    auth_data: HashedPassword,
    salt: Salt,
    username: &str,
    save_pwd: &[u8],
) -> Result<()> {
    let hash_stage_2 = mysql_native_password_hash(save_pwd);
    auth_mysql_with_hash_stage_2(auth_data, salt, username, &hash_stage_2)
}

pub(crate) fn auth_mysql_with_hash_stage_2(
    auth_data: HashedPassword,
    salt: Salt,
    username: &str,
    hash_stage_2: &[u8],
) -> Result<()> {
    ensure!(
        auth_data.len() == 20,
        IllegalParamSnafu {
            msg: "Illegal mysql password length"
        }
    );
    ensure!(
        hash_stage_2.len() == 20,
        InvalidConfigSnafu {
            value: hash_stage_2.len().to_string(),
            msg: "Illegal mysql native password verifier length",
        }
    );
    // ref: https://github.com/mysql/mysql-server/blob/a246bad76b9271cb4333634e954040a970222e0a/sql/auth/password.cc#L62
    let tmp = sha1_two(salt, hash_stage_2);
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

pub fn mysql_native_password_hash(save_pwd: &[u8]) -> Vec<u8> {
    double_sha1(save_pwd)
}

pub fn format_mysql_native_password_verifier(password: &[u8]) -> String {
    format!(
        "mysql_native_password:{}",
        hex::encode(mysql_native_password_hash(password))
    )
}

pub fn format_pbkdf2_sha256_password_verifier(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> Result<String> {
    ensure!(
        iterations > 0 && iterations <= MAX_PBKDF2_SHA256_ITERATIONS,
        IllegalParamSnafu {
            msg: format!(
                "pbkdf2_sha256 iterations must be in 1..={}",
                MAX_PBKDF2_SHA256_ITERATIONS
            )
        }
    );
    ensure!(
        !salt.is_empty() && salt.len() <= MAX_PBKDF2_SHA256_SALT_LEN,
        IllegalParamSnafu {
            msg: format!(
                "pbkdf2_sha256 salt length must be in 1..={}",
                MAX_PBKDF2_SHA256_SALT_LEN
            )
        }
    );

    let mut hash = [0u8; PBKDF2_SHA256_HASH_LEN];
    pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut hash);
    Ok(format!(
        "pbkdf2_sha256:{iterations}:{}:{}",
        hex::encode(salt),
        hex::encode(hash)
    ))
}

#[derive(Clone, PartialEq, Eq)]
pub struct PgScramSha256Verifier {
    iterations: u32,
    salt: Vec<u8>,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
}

impl std::fmt::Debug for PgScramSha256Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgScramSha256Verifier")
            .field("iterations", &self.iterations)
            .field("salt", &"<REDACTED>")
            .field("stored_key", &"<REDACTED>")
            .field("server_key", &"<REDACTED>")
            .finish()
    }
}

impl PgScramSha256Verifier {
    pub fn new(
        iterations: u32,
        salt: Vec<u8>,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    ) -> Result<Self> {
        ensure!(
            iterations > 0 && iterations <= MAX_PBKDF2_SHA256_ITERATIONS,
            IllegalParamSnafu {
                msg: format!(
                    "pg_scram_sha256 iterations must be in 1..={}",
                    MAX_PBKDF2_SHA256_ITERATIONS
                )
            }
        );
        ensure!(
            !salt.is_empty() && salt.len() <= MAX_PBKDF2_SHA256_SALT_LEN,
            IllegalParamSnafu {
                msg: format!(
                    "pg_scram_sha256 salt length must be in 1..={}",
                    MAX_PBKDF2_SHA256_SALT_LEN
                )
            }
        );
        ensure!(
            stored_key.len() == PG_SCRAM_SHA256_KEY_LEN,
            IllegalParamSnafu {
                msg: "pg_scram_sha256 stored key must be 32 bytes"
            }
        );
        ensure!(
            server_key.len() == PG_SCRAM_SHA256_KEY_LEN,
            IllegalParamSnafu {
                msg: "pg_scram_sha256 server key must be 32 bytes"
            }
        );

        Ok(Self {
            iterations,
            salt,
            stored_key,
            server_key,
        })
    }

    pub fn from_password(password: &[u8], salt: &[u8], iterations: u32) -> Result<Self> {
        let salted_password = pg_scram_sha256_salted_password(password, salt, iterations)?;
        Self::from_salted_password(salted_password, salt.to_vec(), iterations)
    }

    pub fn from_salted_password(
        salted_password: Vec<u8>,
        salt: Vec<u8>,
        iterations: u32,
    ) -> Result<Self> {
        ensure!(
            salted_password.len() == PG_SCRAM_SHA256_KEY_LEN,
            IllegalParamSnafu {
                msg: "pg_scram_sha256 salted password must be 32 bytes"
            }
        );
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        Self::new(iterations, salt, stored_key, server_key)
    }

    pub fn iterations(&self) -> u32 {
        self.iterations
    }

    pub fn salt(&self) -> &[u8] {
        &self.salt
    }

    pub fn verify_plain_password(&self, password: &[u8]) -> Result<bool> {
        let salted_password =
            pg_scram_sha256_salted_password(password, &self.salt, self.iterations)?;
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        Ok(self.stored_key.ct_eq(&stored_key).into())
    }

    pub fn verify_client_proof(
        &self,
        auth_message: &[u8],
        client_proof: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        if client_proof.len() != PG_SCRAM_SHA256_KEY_LEN {
            return Ok(None);
        }

        let client_signature = hmac_sha256(&self.stored_key, auth_message);
        let client_key = xor(client_proof, &client_signature);
        let stored_key = sha256(&client_key);
        if self.stored_key.ct_eq(&stored_key).into() {
            Ok(Some(hmac_sha256(&self.server_key, auth_message)))
        } else {
            Ok(None)
        }
    }
}

pub fn format_pg_scram_sha256_password_verifier(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> Result<String> {
    let verifier = PgScramSha256Verifier::from_password(password, salt, iterations)?;
    Ok(format!(
        "pg_scram_sha256:{iterations}:{}:{}:{}",
        hex::encode(verifier.salt),
        hex::encode(verifier.stored_key),
        hex::encode(verifier.server_key)
    ))
}

fn pg_scram_sha256_salted_password(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> Result<Vec<u8>> {
    ensure!(
        iterations > 0 && iterations <= MAX_PBKDF2_SHA256_ITERATIONS,
        IllegalParamSnafu {
            msg: format!(
                "pg_scram_sha256 iterations must be in 1..={}",
                MAX_PBKDF2_SHA256_ITERATIONS
            )
        }
    );
    ensure!(
        !salt.is_empty() && salt.len() <= MAX_PBKDF2_SHA256_SALT_LEN,
        IllegalParamSnafu {
            msg: format!(
                "pg_scram_sha256 salt length must be in 1..={}",
                MAX_PBKDF2_SHA256_SALT_LEN
            )
        }
    );

    let mut salted_password = [0u8; PG_SCRAM_SHA256_KEY_LEN];
    pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut salted_password);
    Ok(salted_password.to_vec())
}

fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

fn sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

fn xor(lhs: &[u8], rhs: &[u8]) -> Vec<u8> {
    lhs.iter().zip(rhs).map(|(l, r)| l ^ r).collect()
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

    #[test]
    fn test_format_mysql_native_password_verifier() {
        let verifier = format_mysql_native_password_verifier("123456".as_bytes());
        assert_eq!(
            "mysql_native_password:6bb4837eb74329105ee4568dda7dc67ed2ca2ad9",
            verifier
        );
    }

    #[test]
    fn test_format_pbkdf2_sha256_password_verifier() {
        let verifier =
            format_pbkdf2_sha256_password_verifier("password".as_bytes(), b"salt", 4096).unwrap();
        assert_eq!(
            "pbkdf2_sha256:4096:73616c74:c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a",
            verifier
        );

        assert!(format_pbkdf2_sha256_password_verifier(b"password", b"", 4096).is_err());
        assert!(format_pbkdf2_sha256_password_verifier(b"password", b"salt", 0).is_err());
        assert!(
            format_pbkdf2_sha256_password_verifier(
                b"password",
                b"salt",
                MAX_PBKDF2_SHA256_ITERATIONS + 1,
            )
            .is_err()
        );
    }

    #[test]
    fn test_format_pg_scram_sha256_password_verifier() {
        let verifier =
            format_pg_scram_sha256_password_verifier("password".as_bytes(), b"salt", 4096).unwrap();
        assert_eq!(
            "pg_scram_sha256:4096:73616c74:945e1c466fc9932efadc23781edc5d1e78d5e10f005933652af1a6105154f084:b9bf0e811b1fb6793671c0cc3adedf7c75cd72291191092ad65878c5a02aad2c",
            verifier
        );
    }
}
