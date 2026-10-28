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

/// Remote database protocol, inferred from the connection DSN scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionScheme {
    MySql,
    Postgres,
}

impl ConnectionScheme {
    /// The IANA-registered default port for this protocol.
    pub fn default_port(self) -> u16 {
        match self {
            ConnectionScheme::MySql => 3306,
            ConnectionScheme::Postgres => 5432,
        }
    }

    /// The DSN scheme string.
    pub fn as_str(self) -> &'static str {
        match self {
            ConnectionScheme::MySql => "mysql",
            ConnectionScheme::Postgres => "postgres",
        }
    }
}

/// A parsed connection DSN.
///
/// This is *not* what gets persisted — the persisted form is the plaintext
/// `RelationalOptions::connection` string. This type exists so the datanode
/// can (a) pick the right remote driver from `scheme` and build the driver config from
/// the structured fields, and (b) render the password redacted via [`Self::redacted`]
/// on every display/log/error path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionInfo {
    pub scheme: ConnectionScheme,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl ConnectionInfo {
    /// Parses a DSN like `mysql://user:password@host:3306/db`.
    pub fn parse(dsn: &str) -> Result<Self, String> {
        let url = url::Url::parse(dsn).map_err(|err| format!("invalid connection DSN: {err}"))?;

        let scheme = match url.scheme() {
            "mysql" => ConnectionScheme::MySql,
            "postgres" | "postgresql" => ConnectionScheme::Postgres,
            other => return Err(format!("unsupported connection scheme: {other}")),
        };

        let host = url
            .host_str()
            .ok_or_else(|| "connection DSN is missing a host".to_string())?
            .to_string();

        Ok(Self {
            scheme,
            host,
            port: url.port().unwrap_or_else(|| scheme.default_port()),
            user: url.username().to_string(),
            password: url.password().unwrap_or_default().to_string(),
            database: url.path().trim_start_matches('/').to_string(),
        })
    }

    /// Renders the DSN with the password replaced by `***`. Use this everywhere the
    /// connection is shown (logs, errors, `SHOW CREATE TABLE`) so the plaintext
    /// password never leaks.
    pub fn redacted(&self) -> String {
        format!(
            "{}://{}:***@{}:{}/{}",
            self.scheme.as_str(),
            self.user,
            self.host,
            self.port,
            self.database
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_mysql_dsn() {
        let info = ConnectionInfo::parse("mysql://root:secret@127.0.0.1:3307/testdb").unwrap();
        assert_eq!(info.scheme, ConnectionScheme::MySql);
        assert_eq!(info.host, "127.0.0.1");
        assert_eq!(info.port, 3307);
        assert_eq!(info.user, "root");
        assert_eq!(info.password, "secret");
        assert_eq!(info.database, "testdb");
    }

    #[test]
    fn test_parse_postgres_uses_default_port() {
        let info = ConnectionInfo::parse("postgresql://alice@db.example.com/mydb").unwrap();
        assert_eq!(info.scheme, ConnectionScheme::Postgres);
        assert_eq!(info.port, 5432);
        assert_eq!(info.user, "alice");
        assert_eq!(info.password, "");
        assert_eq!(info.database, "mydb");
    }

    #[test]
    fn test_redacted_hides_password() {
        let info = ConnectionInfo::parse("mysql://root:s3cret@db.example.com:3306/db").unwrap();
        let redacted = info.redacted();
        assert!(redacted.contains(":***@"));
        assert!(!redacted.contains("s3cret"));
    }

    #[test]
    fn test_parse_rejects_unsupported_scheme() {
        let err = ConnectionInfo::parse("sqlite:///tmp/x.db").unwrap_err();
        assert!(err.contains("unsupported connection scheme"));
    }
}
