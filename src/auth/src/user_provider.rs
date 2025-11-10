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
use snafu::{OptionExt, ResultExt, ensure};

use crate::common::{Identity, Password};
use crate::error::{
    IllegalParamSnafu, InvalidConfigSnafu, IoSnafu, Result, UnsupportedPasswordTypeSnafu,
    UserNotFoundSnafu, UserPasswordMismatchSnafu,
};
use crate::user_info::{DefaultUserInfo, PermissionMode};
use crate::{UserInfoRef, auth_mysql};

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

/// Type alias for user info map
/// Key is username, value is (password, permission_mode)
pub type UserInfoMap = HashMap<String, (Vec<u8>, PermissionMode)>;

fn load_credential_from_file(filepath: &str) -> Result<UserInfoMap> {
    // check valid path
    let path = Path::new(filepath);
    if !path.exists() {
        return InvalidConfigSnafu {
            value: filepath.to_string(),
            msg: "UserProvider file must exist",
        }
        .fail();
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
            // The line format is:
            // - `username=password` - Basic user with default permissions
            // - `username:permission_mode=password` - User with specific permission mode
            // - Lines starting with '#' are treated as comments and ignored
            // - Empty lines are ignored
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }

            parse_credential_line(line)
        })
        .collect::<HashMap<String, _>>();

    ensure!(
        !credential.is_empty(),
        InvalidConfigSnafu {
            value: filepath,
            msg: "UserProvider's file must contains at least one valid credential",
        }
    );

    Ok(credential)
}

/// Parse a line of credential in the format of `username=password` or `username:permission_mode=password`
pub(crate) fn parse_credential_line(line: &str) -> Option<(String, (Vec<u8>, PermissionMode))> {
    let parts = line.split('=').collect::<Vec<&str>>();
    if parts.len() != 2 {
        return None;
    }

    let (username_part, password) = (parts[0], parts[1]);
    let (username, permission_mode) = if let Some((user, perm)) = username_part.split_once(':') {
        (user, PermissionMode::from_str(perm))
    } else {
        (username_part, PermissionMode::default())
    };

    Some((
        username.to_string(),
        (password.as_bytes().to_vec(), permission_mode),
    ))
}

fn authenticate_with_credential(
    users: &UserInfoMap,
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
            let (save_pwd, permission_mode) = users.get(username).context(UserNotFoundSnafu {
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
                        Ok(DefaultUserInfo::with_name_and_permission(
                            username,
                            *permission_mode,
                        ))
                    } else {
                        UserPasswordMismatchSnafu {
                            username: username.to_string(),
                        }
                        .fail()
                    }
                }
                Password::MysqlNativePassword(auth_data, salt) => {
                    auth_mysql(auth_data, salt, username, save_pwd).map(|_| {
                        DefaultUserInfo::with_name_and_permission(username, *permission_mode)
                    })
                }
                Password::PgMD5(_, _) => UnsupportedPasswordTypeSnafu {
                    password_type: "pg_md5",
                }
                .fail(),
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_credential_line() {
        // Basic username=password format
        let result = parse_credential_line("admin=password123");
        assert_eq!(
            result,
            Some((
                "admin".to_string(),
                ("password123".as_bytes().to_vec(), PermissionMode::default())
            ))
        );

        // Username with permission mode
        let result = parse_credential_line("user:ReadOnly=secret");
        assert_eq!(
            result,
            Some((
                "user".to_string(),
                ("secret".as_bytes().to_vec(), PermissionMode::ReadOnly)
            ))
        );
        let result = parse_credential_line("user:ro=secret");
        assert_eq!(
            result,
            Some((
                "user".to_string(),
                ("secret".as_bytes().to_vec(), PermissionMode::ReadOnly)
            ))
        );
        // Username with WriteOnly permission mode
        let result = parse_credential_line("writer:WriteOnly=mypass");
        assert_eq!(
            result,
            Some((
                "writer".to_string(),
                ("mypass".as_bytes().to_vec(), PermissionMode::WriteOnly)
            ))
        );

        // Username with 'wo' as WriteOnly permission shorthand
        let result = parse_credential_line("writer:wo=mypass");
        assert_eq!(
            result,
            Some((
                "writer".to_string(),
                ("mypass".as_bytes().to_vec(), PermissionMode::WriteOnly)
            ))
        );

        // Username with complex password containing special characters
        let result = parse_credential_line("admin:rw=p@ssw0rd!123");
        assert_eq!(
            result,
            Some((
                "admin".to_string(),
                (
                    "p@ssw0rd!123".as_bytes().to_vec(),
                    PermissionMode::ReadWrite
                )
            ))
        );

        // Username with spaces should be preserved
        let result = parse_credential_line("user name:WriteOnly=password");
        assert_eq!(
            result,
            Some((
                "user name".to_string(),
                ("password".as_bytes().to_vec(), PermissionMode::WriteOnly)
            ))
        );

        // Invalid format - no equals sign
        let result = parse_credential_line("invalid_line");
        assert_eq!(result, None);

        // Invalid format - multiple equals signs
        let result = parse_credential_line("user=pass=word");
        assert_eq!(result, None);

        // Empty password
        let result = parse_credential_line("user=");
        assert_eq!(
            result,
            Some((
                "user".to_string(),
                ("".as_bytes().to_vec(), PermissionMode::default())
            ))
        );

        // Empty username
        let result = parse_credential_line("=password");
        assert_eq!(
            result,
            Some((
                "".to_string(),
                ("password".as_bytes().to_vec(), PermissionMode::default())
            ))
        );
    }
}
