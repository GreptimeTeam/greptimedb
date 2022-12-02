pub mod mysql;
pub mod postgres;
pub mod pwd;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use self::mysql::MysqlAuthPlugin;
use self::postgres::PgAuthPlugin;
use crate::error::Result;

#[async_trait::async_trait]
pub trait UserProvider: Send + Sync {
    fn name(&self) -> String;

    async fn get_user_info(&self, identity: &Identity) -> Result<Option<UserInfo>>;
}

pub type UserProviderRef = Arc<dyn UserProvider>;

type Username = String;
type HostOrIp = String;

#[derive(Debug)]
pub enum Identity {
    UserId(Username, Option<HostOrIp>),
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    username: String,
    auth_methods: Vec<AuthMethod>,
    // TODO(fys): maybe contain some user permission information here
}

#[derive(Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    PlainText(Vec<u8>),
    DoubleSha1(Vec<u8>),
}

impl UserInfo {
    pub fn new(username: impl Into<String>, auth_methods: Vec<AuthMethod>) -> Self {
        Self {
            username: username.into(),
            auth_methods,
        }
    }

    pub fn get_username(&self) -> String {
        self.username.clone()
    }

    pub fn auth_methods(&self) -> &[AuthMethod] {
        &self.auth_methods
    }

    pub fn mysql_auth_method(&self, auth_plugin: &MysqlAuthPlugin) -> Option<&AuthMethod> {
        self.auth_methods
            .iter()
            .find(|&method| method.support_mysql(auth_plugin))
    }

    pub fn pg_auth_method(&self, auth_plugin: &PgAuthPlugin) -> Option<&AuthMethod> {
        self.auth_methods
            .iter()
            .find(|&method| method.support_pg(auth_plugin))
    }
}

impl AuthMethod {
    /// Get the auth method that supports the specified mysql auth plugin.
    fn support_mysql(&self, auth_plugin: &MysqlAuthPlugin) -> bool {
        match auth_plugin {
            MysqlAuthPlugin::MysqlNativePwd => match self {
                AuthMethod::PlainText(_) => true,
                AuthMethod::DoubleSha1(_) => true,
            },
        }
    }

    /// Get the authentication method that supports the specified pg auth plugin.
    fn support_pg(&self, auth_plugin: &PgAuthPlugin) -> bool {
        match auth_plugin {
            PgAuthPlugin::PlainText => match self {
                AuthMethod::PlainText(_) => true,
                AuthMethod::DoubleSha1(_) => true,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AuthMethod;
    use crate::auth::mysql::MysqlAuthPlugin;
    use crate::auth::postgres::PgAuthPlugin;
    use crate::auth::pwd::sha1;

    #[test]
    fn test_auth_method_support() {
        let plain = AuthMethod::PlainText(b"123456".to_vec());
        assert!(plain.support_mysql(&MysqlAuthPlugin::MysqlNativePwd));
        assert!(plain.support_pg(&PgAuthPlugin::PlainText));

        let doublesha1 = AuthMethod::DoubleSha1(sha1(&sha1(b"123456")).to_vec());
        assert!(doublesha1.support_mysql(&MysqlAuthPlugin::MysqlNativePwd));
        assert!(doublesha1.support_pg(&PgAuthPlugin::PlainText));
    }
}
