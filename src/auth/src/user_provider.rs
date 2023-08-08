pub(crate) mod static_user_provider;

use secrecy::SecretString;

use crate::error::Result;
use crate::UserInfo;

#[async_trait::async_trait]
pub trait UserProvider: Send + Sync {
    fn name(&self) -> &str;

    /// [`authenticate`] checks whether a user is valid and allowed to access the database.
    async fn authenticate(&self, id: Identity<'_>, password: Password<'_>) -> Result<UserInfo>;

    /// [`authorize`] checks whether a connection request
    /// from a certain user to a certain catalog/schema is legal.
    /// This method should be called after [`authenticate`].
    async fn authorize(&self, catalog: &str, schema: &str, user_info: &UserInfo) -> Result<()>;

    /// [`auth`] is a combination of [`authenticate`] and [`authorize`].
    /// In most cases it's preferred for both convenience and performance.
    async fn auth(
        &self,
        id: Identity<'_>,
        password: Password<'_>,
        catalog: &str,
        schema: &str,
    ) -> Result<UserInfo> {
        let user_info = self.authenticate(id, password).await?;
        self.authorize(catalog, schema, &user_info).await?;
        Ok(user_info)
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
