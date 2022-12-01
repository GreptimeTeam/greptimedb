mod mysql;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use self::mysql::{mysql_native_pwd_auth1, mysql_native_pwd_auth2};
use crate::error::Result;

#[async_trait::async_trait]
pub trait UserProvider: Send + Sync {
    fn name(&self) -> String;

    async fn get_user_info(&self, certificate: Certificate) -> Result<Option<UserInfo>>;
}

pub type UserProviderRef = Arc<dyn UserProvider>;

type UserName = String;
type HostOrIp = String;

#[derive(Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    PlainPwd(Vec<u8>),
    DoubleSha1(Vec<u8>),
}

pub enum Certificate {
    UserId(UserName, HostOrIp),
}

// When auth_methods.len() == 0, it means that no authentication is required.
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    username: Option<String>,
    auth_methods: Vec<AuthMethod>,
}

impl UserInfo {
    pub fn new(username: Option<String>, auth_methods: Vec<AuthMethod>) -> Self {
        Self {
            username,
            auth_methods,
        }
    }

    pub fn get_username(&self) -> Option<String> {
        self.username.clone()
    }

    pub fn auth_methods(&self) -> &[AuthMethod] {
        &self.auth_methods
    }
}

pub enum MysqlAuthPlugin {
    MysqlNativePwd,
}

/// # Arguments
///
/// * `auth_plugin`  - the type of mysql auth plugin, include **mysql_native_password**.
/// * `hashed_value` - the hashed string passed from mysql client.
/// * `salt`         - the random string generated by the server.
/// * `auth_methods` - it is used to auth **hashed_value** and **salt**.
pub fn auth_mysql(
    auth_plugin: MysqlAuthPlugin,
    hashed_value: &[u8],
    salt: &[u8],
    auth_methods: &[AuthMethod],
) -> bool {
    match auth_plugin {
        MysqlAuthPlugin::MysqlNativePwd =>
        {
            #[allow(clippy::never_loop)]
            for method in auth_methods {
                match method {
                    AuthMethod::PlainPwd(pwd) => {
                        return mysql_native_pwd_auth2(hashed_value, salt, pwd)
                    }
                    AuthMethod::DoubleSha1(hashed_stage2) => {
                        return mysql_native_pwd_auth1(hashed_value, salt, hashed_stage2)
                    }
                }
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::{auth_mysql, AuthMethod, MysqlAuthPlugin};

    #[test]
    fn test_auth_method() {
        let hash_val = mock_hashed_val();
        let salt = b"1213hjkasdhjkashdjka";

        assert!(auth_mysql(
            MysqlAuthPlugin::MysqlNativePwd,
            &hash_val,
            salt,
            &[AuthMethod::PlainPwd(b"123456".to_vec())],
        ));

        assert!(auth_mysql(
            MysqlAuthPlugin::MysqlNativePwd,
            &hash_val,
            salt,
            &[AuthMethod::DoubleSha1(mock_hashed_stage2())],
        ));
    }

    // mock hashed value passes from mysql client
    // sha1(pwd) xor sha1(slat + sha1(sha1(pwd)))
    // pwd = b"123456"
    // salt = b"1213hjkasdhjkashdjka"
    fn mock_hashed_val() -> Vec<u8> {
        vec![
            5, 55, 38, 182, 222, 78, 102, 175, 150, 55, 108, 219, 212, 225, 164, 108, 140, 86, 218,
            56,
        ]
    }

    // sha1(sha1(pwd))
    // pwd = b"123456"
    fn mock_hashed_stage2() -> Vec<u8> {
        vec![
            107, 180, 131, 126, 183, 67, 41, 16, 94, 228, 86, 141, 218, 125, 198, 126, 210, 202,
            42, 217,
        ]
    }
}
