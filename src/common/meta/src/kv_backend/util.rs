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

use std::sync::LazyLock;

use regex::{Captures, Regex};
use url::Url;

const REDACTED: &str = "***";

static SENSITIVE_KV_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)(^|\s)(password|pass|pwd|token|secret)\s*=\s*('[^']*'|"[^"]*"|\S+)"#).unwrap()
});

/// Removes sensitive information like passwords from connection strings.
pub fn sanitize_connection_string(conn_str: &str) -> String {
    if let Ok(url) = Url::parse(conn_str) {
        return sanitize_url(url);
    }

    sanitize_key_value_connection_string(conn_str)
}

fn sanitize_url(mut url: Url) -> String {
    if !url.username().is_empty() {
        let _ = url.set_username("");
    }
    if url.password().is_some() {
        let _ = url.set_password(None);
    }

    if url.query().is_some() {
        let pairs = url
            .query_pairs()
            .map(|(key, value)| {
                let value = if is_sensitive_key(&key) {
                    REDACTED.into()
                } else {
                    value
                };
                (key.into_owned(), value.into_owned())
            })
            .collect::<Vec<_>>();

        url.query_pairs_mut().clear().extend_pairs(pairs);
    }

    url.as_str()
        .split_once("://")
        .map(|(_, addr)| addr.to_string())
        .unwrap_or_else(|| url.to_string())
}

fn sanitize_key_value_connection_string(conn_str: &str) -> String {
    SENSITIVE_KV_RE
        .replace_all(conn_str, |caps: &Captures<'_>| {
            format!("{}{}={}", &caps[1], &caps[2], REDACTED)
        })
        .into_owned()
}

fn is_sensitive_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "password" | "pass" | "pwd" | "token" | "secret"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_connection_string() {
        let password = "sensitive-value";

        let conn_str = format!("mysql://user:{password}@localhost:3306/db");
        assert_eq!(sanitize_connection_string(&conn_str), "localhost:3306/db");

        let conn_str = "mysql://localhost:3306/db";
        assert_eq!(sanitize_connection_string(conn_str), "localhost:3306/db");

        let conn_str =
            format!("host=localhost port=5432 user=postgres password={password} dbname=mydb");
        assert_eq!(
            sanitize_connection_string(&conn_str),
            "host=localhost port=5432 user=postgres password=*** dbname=mydb"
        );

        let conn_str =
            format!("host=localhost port=5432 user=postgres PASSWORD={password} dbname=mydb");
        assert_eq!(
            sanitize_connection_string(&conn_str),
            "host=localhost port=5432 user=postgres PASSWORD=*** dbname=mydb"
        );

        let conn_str =
            format!("host=localhost port=5432 user=postgres password = {password} dbname=mydb");
        assert_eq!(
            sanitize_connection_string(&conn_str),
            "host=localhost port=5432 user=postgres password=*** dbname=mydb"
        );

        let conn_str =
            format!("host=localhost port=5432 password='{password} with spaces' dbname=mydb");
        assert_eq!(
            sanitize_connection_string(&conn_str),
            "host=localhost port=5432 password=*** dbname=mydb"
        );

        let conn_str = format!("mysql://localhost:3306/db?password={password}&ssl-mode=VERIFY_CA");
        assert_eq!(
            sanitize_connection_string(&conn_str),
            "localhost:3306/db?password=***&ssl-mode=VERIFY_CA"
        );

        let conn_str =
            format!("mysql://localhost:3306/db?token={password}&secret={password}&user=root");
        assert_eq!(
            sanitize_connection_string(&conn_str),
            "localhost:3306/db?token=***&secret=***&user=root"
        );

        let conn_str =
            format!("host=localhost port=5432 token={password} secret={password} dbname=mydb");
        assert_eq!(
            sanitize_connection_string(&conn_str),
            "host=localhost port=5432 token=*** secret=*** dbname=mydb"
        );

        let conn_str = "host=localhost port=5432 user=postgres dbname=mydb";
        assert_eq!(
            sanitize_connection_string(conn_str),
            "host=localhost port=5432 user=postgres dbname=mydb"
        );
    }
}
