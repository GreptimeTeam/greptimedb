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
use std::io::BufRead;
use std::path::Path;
use std::{fmt, io};

use common_base::secrets::ExposeSecret;
use pbkdf2::pbkdf2_hmac;
use sha2::Sha256;
use snafu::{OptionExt, ResultExt, ensure};
use subtle::ConstantTimeEq;

use crate::common::{
    Identity, MAX_PBKDF2_SHA256_ITERATIONS, MAX_PBKDF2_SHA256_SALT_LEN, PBKDF2_SHA256_HASH_LEN,
    PG_SCRAM_SHA256_KEY_LEN, Password, PgScramSha256Verifier, auth_mysql_with_hash_stage_2,
};
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

    async fn postgres_auth_info(&self, _id: Identity<'_>) -> Result<PgAuthInfo> {
        Ok(PgAuthInfo::Cleartext)
    }

    /// Returns whether this user provider implementation is backed by an external system.
    fn external(&self) -> bool {
        false
    }
}

pub enum PgAuthInfo {
    ScramSha256 {
        verifier: PgScramSha256Verifier,
        user_info: Option<UserInfoRef>,
    },
    Cleartext,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum PasswordVerifier {
    PlainText(String),
    Pbkdf2Sha256 {
        iterations: u32,
        salt: Vec<u8>,
        hash: Vec<u8>,
    },
    MysqlNativePassword {
        hash_stage_2: Vec<u8>,
    },
    PgScramSha256(PgScramSha256Verifier),
}

impl fmt::Debug for PasswordVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PasswordVerifier::PlainText(_) => {
                f.debug_tuple("PlainText").field(&"<REDACTED>").finish()
            }
            PasswordVerifier::Pbkdf2Sha256 { iterations, .. } => f
                .debug_struct("Pbkdf2Sha256")
                .field("iterations", iterations)
                .field("salt", &"<REDACTED>")
                .field("hash", &"<REDACTED>")
                .finish(),
            PasswordVerifier::MysqlNativePassword { .. } => f
                .debug_struct("MysqlNativePassword")
                .field("hash_stage_2", &"<REDACTED>")
                .finish(),
            PasswordVerifier::PgScramSha256(_) => f
                .debug_struct("PgScramSha256")
                .field("verifier", &"<REDACTED>")
                .finish(),
        }
    }
}

impl PasswordVerifier {
    fn parse(input: &str) -> Option<Self> {
        if let Some(password) = input.strip_prefix("plain:") {
            return Some(Self::PlainText(password.to_string()));
        }

        if let Some(verifier) = input.strip_prefix("pbkdf2_sha256:") {
            let mut parts = verifier.split(':');
            let iterations = parts.next()?.parse::<u32>().ok()?;
            let salt = hex::decode(parts.next()?).ok()?;
            let hash = hex::decode(parts.next()?).ok()?;
            if parts.next().is_some()
                || iterations == 0
                || iterations > MAX_PBKDF2_SHA256_ITERATIONS
                || salt.is_empty()
                || salt.len() > MAX_PBKDF2_SHA256_SALT_LEN
                || hash.len() != PBKDF2_SHA256_HASH_LEN
            {
                return None;
            }

            return Some(Self::Pbkdf2Sha256 {
                iterations,
                salt,
                hash,
            });
        }

        if let Some(verifier) = input.strip_prefix("mysql_native_password:") {
            let hash_stage_2 = hex::decode(verifier).ok()?;
            if hash_stage_2.len() != 20 {
                return None;
            }

            return Some(Self::MysqlNativePassword { hash_stage_2 });
        }

        if let Some(verifier) = input.strip_prefix("pg_scram_sha256:") {
            let mut parts = verifier.split(':');
            let iterations = parts.next()?.parse::<u32>().ok()?;
            let salt = hex::decode(parts.next()?).ok()?;
            let stored_key = hex::decode(parts.next()?).ok()?;
            let server_key = hex::decode(parts.next()?).ok()?;
            if parts.next().is_some()
                || stored_key.len() != PG_SCRAM_SHA256_KEY_LEN
                || server_key.len() != PG_SCRAM_SHA256_KEY_LEN
            {
                return None;
            }

            return PgScramSha256Verifier::new(iterations, salt, stored_key, server_key)
                .ok()
                .map(Self::PgScramSha256);
        }

        Some(Self::PlainText(input.to_string()))
    }

    fn supports_pg_scram_sha256(&self) -> bool {
        !matches!(self, PasswordVerifier::MysqlNativePassword { .. })
    }

    fn to_pg_scram_sha256_verifier(&self) -> Option<PgScramSha256Verifier> {
        match self {
            PasswordVerifier::PlainText(password) => {
                let salt = rand::random::<[u8; PG_SCRAM_SHA256_KEY_LEN]>();
                PgScramSha256Verifier::from_password(
                    password.as_bytes(),
                    &salt,
                    crate::DEFAULT_PBKDF2_SHA256_ITERATIONS,
                )
                .ok()
            }
            PasswordVerifier::Pbkdf2Sha256 {
                iterations,
                salt,
                hash,
            } => {
                PgScramSha256Verifier::from_salted_password(hash.clone(), salt.clone(), *iterations)
                    .ok()
            }
            PasswordVerifier::PgScramSha256(verifier) => Some(verifier.clone()),
            PasswordVerifier::MysqlNativePassword { .. } => None,
        }
    }

    fn verify_plain_text(&self, password: &str) -> bool {
        match self {
            PasswordVerifier::PlainText(expected) => {
                expected.as_bytes().ct_eq(password.as_bytes()).into()
            }
            PasswordVerifier::Pbkdf2Sha256 {
                iterations,
                salt,
                hash,
            } => {
                if hash.len() != PBKDF2_SHA256_HASH_LEN {
                    return false;
                }
                let mut actual = [0u8; PBKDF2_SHA256_HASH_LEN];
                pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, *iterations, &mut actual);
                hash.as_slice().ct_eq(&actual[..]).into()
            }
            PasswordVerifier::MysqlNativePassword { .. } => false,
            PasswordVerifier::PgScramSha256(verifier) => verifier
                .verify_plain_password(password.as_bytes())
                .unwrap_or(false),
        }
    }

    fn verify_mysql_native_password(
        &self,
        auth_data: &[u8],
        salt: &[u8],
        username: &str,
    ) -> Result<()> {
        match self {
            PasswordVerifier::PlainText(password) => {
                auth_mysql(auth_data, salt, username, password.as_bytes())
            }
            PasswordVerifier::MysqlNativePassword { hash_stage_2 } => {
                auth_mysql_with_hash_stage_2(auth_data, salt, username, hash_stage_2)
            }
            PasswordVerifier::Pbkdf2Sha256 { .. } => UnsupportedPasswordTypeSnafu {
                password_type: "mysql_native_password_with_pbkdf2_sha256_verifier",
            }
            .fail(),
            PasswordVerifier::PgScramSha256(_) => UnsupportedPasswordTypeSnafu {
                password_type: "mysql_native_password_with_pg_scram_sha256_verifier",
            }
            .fail(),
        }
    }
}

/// Type alias for user info map.
/// Key is username, value is (password verifier, permission_mode).
pub type UserInfoMap = HashMap<String, (PasswordVerifier, PermissionMode)>;

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

/// Parse a line of credential in the format of `username=password` or `username:permission_mode=password`.
///
/// The password part accepts legacy plain text and explicit verifier formats:
/// - `plain:<password>`
/// - `pbkdf2_sha256:<iterations>:<hex-encoded-salt>:<hex-encoded-hash>`
/// - `mysql_native_password:<hex-encoded-sha1-sha1-password>`
/// - `pg_scram_sha256:<iterations>:<hex-encoded-salt>:<hex-encoded-stored-key>:<hex-encoded-server-key>`
pub(crate) fn parse_credential_line(
    line: &str,
) -> Option<(String, (PasswordVerifier, PermissionMode))> {
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

    let verifier = PasswordVerifier::parse(password)?;

    Some((username.to_string(), (verifier, permission_mode)))
}

pub(crate) fn postgres_auth_info_with_credential(
    users: &UserInfoMap,
    input_id: Identity<'_>,
) -> Result<PgAuthInfo> {
    match input_id {
        Identity::UserId(username, _) => {
            ensure!(
                !username.is_empty(),
                IllegalParamSnafu {
                    msg: "blank username"
                }
            );

            if !users
                .values()
                .all(|(verifier, _)| verifier.supports_pg_scram_sha256())
            {
                // PostgreSQL chooses one auth method during startup. Selecting it
                // per username would expose user or verifier existence.
                return Ok(PgAuthInfo::Cleartext);
            }

            if let Some((verifier, permission_mode)) = users.get(username) {
                if let Some(verifier) = verifier.to_pg_scram_sha256_verifier() {
                    return Ok(PgAuthInfo::ScramSha256 {
                        verifier,
                        user_info: Some(DefaultUserInfo::with_name_and_permission(
                            username,
                            *permission_mode,
                        )),
                    });
                }

                return Ok(PgAuthInfo::Cleartext);
            }

            // Unknown user: hand back a deterministic mock verifier so the SCRAM
            // handshake is indistinguishable from a real user, without running
            // PBKDF2 or leaking existence through an unstable salt.
            Ok(PgAuthInfo::ScramSha256 {
                verifier: PgScramSha256Verifier::mock_for_unknown_user(username.as_bytes()),
                user_info: None,
            })
        }
    }
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
            let (verifier, permission_mode) = users.get(username).context(UserNotFoundSnafu {
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
                    if verifier.verify_plain_text(pwd.expose_secret()) {
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
                Password::MysqlNativePassword(auth_data, salt) => verifier
                    .verify_mysql_native_password(auth_data, salt, username)
                    .map(|_| DefaultUserInfo::with_name_and_permission(username, *permission_mode)),
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
    use digest::Digest;
    use sha1::Sha1;

    use super::*;
    use crate::common::{format_pg_scram_sha256_password_verifier, mysql_native_password_hash};

    fn plain(password: &str) -> PasswordVerifier {
        PasswordVerifier::PlainText(password.to_string())
    }

    fn sha1_one(data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha1::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }

    fn mysql_native_password_auth_data(password: &str, salt: &[u8]) -> Vec<u8> {
        let hash_stage_1 = sha1_one(password.as_bytes());
        let hash_stage_2 = mysql_native_password_hash(password.as_bytes());
        let mut hasher = Sha1::new();
        hasher.update(salt);
        hasher.update(hash_stage_2);
        let scramble = hasher.finalize();

        hash_stage_1
            .iter()
            .zip(scramble.iter())
            .map(|(lhs, rhs)| lhs ^ rhs)
            .collect()
    }

    #[test]
    fn test_parse_credential_line() {
        // Basic username=password format
        let result = parse_credential_line("admin=password123");
        assert_eq!(
            result,
            Some((
                "admin".to_string(),
                (plain("password123"), PermissionMode::default())
            ))
        );

        // Username with permission mode
        let result = parse_credential_line("user:ReadOnly=secret");
        assert_eq!(
            result,
            Some((
                "user".to_string(),
                (plain("secret"), PermissionMode::ReadOnly)
            ))
        );
        let result = parse_credential_line("user:ro=secret");
        assert_eq!(
            result,
            Some((
                "user".to_string(),
                (plain("secret"), PermissionMode::ReadOnly)
            ))
        );
        // Username with WriteOnly permission mode
        let result = parse_credential_line("writer:WriteOnly=mypass");
        assert_eq!(
            result,
            Some((
                "writer".to_string(),
                (plain("mypass"), PermissionMode::WriteOnly)
            ))
        );

        // Username with 'wo' as WriteOnly permission shorthand
        let result = parse_credential_line("writer:wo=mypass");
        assert_eq!(
            result,
            Some((
                "writer".to_string(),
                (plain("mypass"), PermissionMode::WriteOnly)
            ))
        );

        // Username with complex password containing special characters
        let result = parse_credential_line("admin:rw=p@ssw0rd!123");
        assert_eq!(
            result,
            Some((
                "admin".to_string(),
                (plain("p@ssw0rd!123"), PermissionMode::ReadWrite)
            ))
        );

        // Username with spaces should be preserved
        let result = parse_credential_line("user name:WriteOnly=password");
        assert_eq!(
            result,
            Some((
                "user name".to_string(),
                (plain("password"), PermissionMode::WriteOnly)
            ))
        );

        let result = parse_credential_line("user=plain:password");
        assert_eq!(
            result,
            Some((
                "user".to_string(),
                (plain("password"), PermissionMode::default())
            ))
        );

        let iterations = 4096;
        let salt = b"salt";
        let mut hash = [0u8; 32];
        pbkdf2_hmac::<Sha256>("password".as_bytes(), salt, iterations, &mut hash);
        let result = parse_credential_line(&format!(
            "user=pbkdf2_sha256:{iterations}:{}:{}",
            hex::encode(salt),
            hex::encode(hash)
        ));
        assert_eq!(
            result,
            Some((
                "user".to_string(),
                (
                    PasswordVerifier::Pbkdf2Sha256 {
                        iterations,
                        salt: salt.to_vec(),
                        hash: hash.to_vec(),
                    },
                    PermissionMode::default()
                )
            ))
        );

        let result = parse_credential_line("user=pbkdf2_sha256:4096:not-hex:abcd");
        assert_eq!(result, None);

        // A well-formed but truncated hash must be rejected: a short hash would let
        // many wrong passwords pass by matching only a few derived bytes.
        let result = parse_credential_line(&format!(
            "user=pbkdf2_sha256:4096:{}:abcd",
            hex::encode(salt)
        ));
        assert_eq!(result, None);

        let result = parse_credential_line(&format!(
            "user=pbkdf2_sha256:{}:{}:{}",
            MAX_PBKDF2_SHA256_ITERATIONS + 1,
            hex::encode(salt),
            hex::encode(hash)
        ));
        assert_eq!(result, None);

        let hash_stage_2 = mysql_native_password_hash("password".as_bytes());
        let result = parse_credential_line(&format!(
            "user=mysql_native_password:{}",
            hex::encode(&hash_stage_2)
        ));
        assert_eq!(
            result,
            Some((
                "user".to_string(),
                (
                    PasswordVerifier::MysqlNativePassword { hash_stage_2 },
                    PermissionMode::default()
                )
            ))
        );

        let result = parse_credential_line("user=mysql_native_password:abcd");
        assert_eq!(result, None);

        let verifier =
            format_pg_scram_sha256_password_verifier(b"password", b"salt", 4096).unwrap();
        let result = parse_credential_line(&format!("user={verifier}"));
        assert!(matches!(
            result,
            Some((
                _,
                (
                    PasswordVerifier::PgScramSha256(PgScramSha256Verifier { .. }),
                    PermissionMode::ReadWrite
                )
            ))
        ));

        let result = parse_credential_line("user=pg_scram_sha256:4096:73616c74:abcd:abcd");
        assert_eq!(result, None);

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
            Some(("user".to_string(), (plain(""), PermissionMode::default())))
        );

        // Empty username
        let result = parse_credential_line("=password");
        assert_eq!(
            result,
            Some((
                "".to_string(),
                (plain("password"), PermissionMode::default())
            ))
        );
    }

    #[test]
    fn test_authenticate_with_mysql_native_password_verifier() {
        let password = "password";
        let salt = b"12345678901234567890";
        let hash_stage_2 = mysql_native_password_hash(password.as_bytes());
        let auth_data = mysql_native_password_auth_data(password, salt);
        let users = HashMap::from([(
            "user".to_string(),
            (
                PasswordVerifier::MysqlNativePassword { hash_stage_2 },
                PermissionMode::default(),
            ),
        )]);

        let result = authenticate_with_credential(
            &users,
            Identity::UserId("user", None),
            Password::MysqlNativePassword(&auth_data, salt),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_authenticate_with_plain_text_mysql_native_password() {
        let password = "password";
        let salt = b"12345678901234567890";
        let auth_data = mysql_native_password_auth_data(password, salt);
        let users = HashMap::from([(
            "user".to_string(),
            (
                PasswordVerifier::PlainText(password.to_string()),
                PermissionMode::default(),
            ),
        )]);

        let result = authenticate_with_credential(
            &users,
            Identity::UserId("user", None),
            Password::MysqlNativePassword(&auth_data, salt),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_pbkdf2_sha256_rejects_mysql_native_password() {
        let password = "password";
        let salt = b"salt";
        let iterations = 4096;
        let mut hash = [0u8; 32];
        pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut hash);
        let users = HashMap::from([(
            "user".to_string(),
            (
                PasswordVerifier::Pbkdf2Sha256 {
                    iterations,
                    salt: salt.to_vec(),
                    hash: hash.to_vec(),
                },
                PermissionMode::default(),
            ),
        )]);
        let mysql_salt = b"12345678901234567890";
        let auth_data = mysql_native_password_auth_data(password, mysql_salt);

        let result = authenticate_with_credential(
            &users,
            Identity::UserId("user", None),
            Password::MysqlNativePassword(&auth_data, mysql_salt),
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_authenticate_with_pg_scram_sha256_verifier_plain_text() {
        let verifier =
            format_pg_scram_sha256_password_verifier(b"password", b"salt", 4096).unwrap();
        let (_, user) = parse_credential_line(&format!("user={verifier}")).unwrap();
        let users = HashMap::from([("user".to_string(), user)]);

        let result = authenticate_with_credential(
            &users,
            Identity::UserId("user", None),
            Password::PlainText("password".to_string().into()),
        );
        assert!(result.is_ok());

        let result = authenticate_with_credential(
            &users,
            Identity::UserId("user", None),
            Password::PlainText("wrong".to_string().into()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_postgres_auth_info_uses_scram_for_unknown_user() {
        let verifier =
            format_pg_scram_sha256_password_verifier(b"password", b"salt", 4096).unwrap();
        let (_, user) = parse_credential_line(&format!("user={verifier}")).unwrap();
        let users = HashMap::from([("user".to_string(), user)]);

        let auth_info =
            postgres_auth_info_with_credential(&users, Identity::UserId("unknown", None)).unwrap();
        assert!(matches!(
            auth_info,
            PgAuthInfo::ScramSha256 {
                user_info: None,
                ..
            }
        ));
    }

    #[test]
    fn test_postgres_auth_info_falls_back_to_cleartext() {
        let hash_stage_2 = mysql_native_password_hash("password".as_bytes());
        let users = HashMap::from([(
            "user".to_string(),
            (
                PasswordVerifier::MysqlNativePassword { hash_stage_2 },
                PermissionMode::default(),
            ),
        )]);

        let auth_info =
            postgres_auth_info_with_credential(&users, Identity::UserId("user", None)).unwrap();
        assert!(matches!(auth_info, PgAuthInfo::Cleartext));

        let auth_info =
            postgres_auth_info_with_credential(&users, Identity::UserId("unknown", None)).unwrap();
        assert!(matches!(auth_info, PgAuthInfo::Cleartext));
    }

    #[test]
    fn test_postgres_auth_info_mixed_verifiers_fall_back_to_cleartext() {
        let verifier =
            format_pg_scram_sha256_password_verifier(b"password", b"salt", 4096).unwrap();
        let (_, scram_user) = parse_credential_line(&format!("scram={verifier}")).unwrap();
        let mysql_user = (
            PasswordVerifier::MysqlNativePassword {
                hash_stage_2: mysql_native_password_hash("password".as_bytes()),
            },
            PermissionMode::default(),
        );
        let users = HashMap::from([
            ("scram".to_string(), scram_user),
            ("mysql".to_string(), mysql_user),
        ]);

        let auth_info =
            postgres_auth_info_with_credential(&users, Identity::UserId("scram", None)).unwrap();
        assert!(matches!(auth_info, PgAuthInfo::Cleartext));

        let auth_info =
            postgres_auth_info_with_credential(&users, Identity::UserId("unknown", None)).unwrap();
        assert!(matches!(auth_info, PgAuthInfo::Cleartext));
    }

    #[test]
    fn test_password_verifier_debug_redacts_secrets() {
        let debug = format!("{:?}", PasswordVerifier::PlainText("secret".to_string()));
        assert!(debug.contains("<REDACTED>"));
        assert!(!debug.contains("secret"));

        let debug = format!(
            "{:?}",
            PasswordVerifier::Pbkdf2Sha256 {
                iterations: 4096,
                salt: b"super-secret-salt".to_vec(),
                hash: b"super-secret-hash".to_vec(),
            }
        );
        assert!(debug.contains("Pbkdf2Sha256"));
        assert!(debug.contains("4096"));
        assert!(!debug.contains("super-secret-salt"));
        assert!(!debug.contains("super-secret-hash"));

        let debug = format!(
            "{:?}",
            PasswordVerifier::MysqlNativePassword {
                hash_stage_2: b"super-secret-hash".to_vec(),
            }
        );
        assert!(debug.contains("MysqlNativePassword"));
        assert!(!debug.contains("super-secret-hash"));
    }
}
