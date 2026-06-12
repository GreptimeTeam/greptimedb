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

#![allow(clippy::print_stdout)]

use std::io;

use auth::{
    DEFAULT_PBKDF2_SHA256_ITERATIONS, MAX_PBKDF2_SHA256_SALT_LEN,
    format_mysql_native_password_verifier, format_pbkdf2_sha256_password_verifier,
};
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use rand::RngCore;
use snafu::ResultExt;

use crate::error::{self, Result};

#[derive(Debug, Parser)]
pub struct Command {
    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

#[derive(Debug, Subcommand)]
pub enum SubCommand {
    /// Generate a password verifier for user_provider.
    HashPassword(HashPasswordCommand),
}

impl Command {
    pub fn run(self) -> Result<()> {
        match self.subcmd {
            SubCommand::HashPassword(cmd) => cmd.run(),
        }
    }
}

#[derive(Debug, Parser)]
#[clap(group(
    ArgGroup::new("password-input")
        .required(true)
        .args(["password", "password_stdin"])
))]
pub struct HashPasswordCommand {
    /// Password verifier format to generate.
    #[clap(long, value_enum, default_value = "pbkdf2_sha256")]
    format: PasswordFormat,

    /// Plaintext password. Prefer --password-stdin to avoid shell history leaks.
    #[clap(long)]
    password: Option<String>,

    /// Read the plaintext password from stdin.
    #[clap(long)]
    password_stdin: bool,

    /// PBKDF2-SHA256 iteration count.
    #[clap(long, default_value_t = DEFAULT_PBKDF2_SHA256_ITERATIONS)]
    iterations: u32,

    /// PBKDF2-SHA256 random salt length in bytes.
    #[clap(long, default_value_t = 16)]
    salt_len: usize,

    /// PBKDF2-SHA256 salt as hex. Mainly useful for deterministic automation.
    #[clap(long)]
    salt_hex: Option<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[clap(rename_all = "snake_case")]
enum PasswordFormat {
    Pbkdf2Sha256,
    MysqlNativePassword,
}

impl HashPasswordCommand {
    fn run(self) -> Result<()> {
        let password = self.read_password()?;
        let verifier = match self.format {
            PasswordFormat::Pbkdf2Sha256 => {
                let salt = self.pbkdf2_salt()?;
                format_pbkdf2_sha256_password_verifier(password.as_bytes(), &salt, self.iterations)
                    .map_err(common_error::ext::BoxedError::new)
                    .context(error::OtherSnafu)?
            }
            PasswordFormat::MysqlNativePassword => {
                format_mysql_native_password_verifier(password.as_bytes())
            }
        };

        println!("{verifier}");
        Ok(())
    }

    fn read_password(&self) -> Result<String> {
        if let Some(password) = self.password.as_ref() {
            return Ok(password.clone());
        }

        let mut password = String::new();
        io::stdin()
            .read_line(&mut password)
            .context(error::FileIoSnafu)?;
        Ok(password.trim_end_matches(['\r', '\n']).to_string())
    }

    fn pbkdf2_salt(&self) -> Result<Vec<u8>> {
        if let Some(salt_hex) = self.salt_hex.as_ref() {
            let salt = hex::decode(salt_hex).map_err(|err| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid --salt-hex: {err}"),
                }
                .build()
            })?;
            Self::ensure_salt_len(salt.len())?;
            return Ok(salt);
        }

        Self::ensure_salt_len(self.salt_len)?;
        let mut salt = vec![0u8; self.salt_len];
        rand::rng().fill_bytes(&mut salt);
        Ok(salt)
    }

    fn ensure_salt_len(salt_len: usize) -> Result<()> {
        if salt_len == 0 || salt_len > MAX_PBKDF2_SHA256_SALT_LEN {
            return error::IllegalConfigSnafu {
                msg: format!("salt length must be in 1..={}", MAX_PBKDF2_SHA256_SALT_LEN),
            }
            .fail();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use auth::MAX_PBKDF2_SHA256_ITERATIONS;

    use super::*;

    #[test]
    fn test_hash_password_command_with_pbkdf2_sha256() {
        let cmd = HashPasswordCommand {
            format: PasswordFormat::Pbkdf2Sha256,
            password: Some("password".to_string()),
            password_stdin: false,
            iterations: 4096,
            salt_len: 16,
            salt_hex: Some("73616c74".to_string()),
        };

        let password = cmd.read_password().unwrap();
        let salt = cmd.pbkdf2_salt().unwrap();
        let verifier =
            format_pbkdf2_sha256_password_verifier(password.as_bytes(), &salt, cmd.iterations)
                .unwrap();

        assert_eq!(
            "pbkdf2_sha256:4096:73616c74:c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a",
            verifier
        );
    }

    #[test]
    fn test_hash_password_command_with_mysql_native_password() {
        let verifier = format_mysql_native_password_verifier("123456".as_bytes());

        assert_eq!(
            "mysql_native_password:6bb4837eb74329105ee4568dda7dc67ed2ca2ad9",
            verifier
        );
    }

    #[test]
    fn test_reject_empty_salt() {
        let cmd = HashPasswordCommand {
            format: PasswordFormat::Pbkdf2Sha256,
            password: Some("password".to_string()),
            password_stdin: false,
            iterations: 4096,
            salt_len: 0,
            salt_hex: None,
        };

        assert!(cmd.pbkdf2_salt().is_err());
    }

    #[test]
    fn test_reject_too_many_iterations() {
        let result = format_pbkdf2_sha256_password_verifier(
            b"password",
            b"salt",
            MAX_PBKDF2_SHA256_ITERATIONS + 1,
        );

        assert!(result.is_err());
    }
}
