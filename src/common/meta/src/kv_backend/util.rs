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

use url::Url;

/// Placeholder substituted for any password found in a connection string.
const REDACTED: &str = "***";

/// Removes sensitive information (passwords) from a connection string before it
/// is logged.
///
/// `store_addrs` entries can be a URL (`mysql://`, `postgresql://`, `http://`
/// etcd) or a libpq keyword DSN (`host=… password=…`). URLs are parsed with the
/// `url` crate and their userinfo and any `password` query parameter redacted;
/// keyword DSNs are handled by [`redact_keyword_password`], which understands
/// optional spaces around `=`, single/double quoting, and backslash escapes.
pub fn sanitize_connection_string(conn_str: &str) -> String {
    match Url::parse(conn_str) {
        Ok(url) => redact_url(url),
        Err(_) => redact_keyword_password(conn_str),
    }
}

/// Redacts the userinfo password and any `password` query parameter of a URL.
fn redact_url(mut url: Url) -> String {
    if url.password().is_some() {
        // set_password only fails when the URL cannot have one (no host); in
        // that case there was nothing to redact anyway.
        let _ = url.set_password(Some(REDACTED));
    }

    if url.query_pairs().any(|(key, _)| key == "password") {
        let redacted: Vec<(String, String)> = url
            .query_pairs()
            .map(|(key, value)| {
                let value = if key == "password" {
                    REDACTED.to_string()
                } else {
                    value.into_owned()
                };
                (key.into_owned(), value)
            })
            .collect();
        url.query_pairs_mut().clear().extend_pairs(redacted);
    }

    url.to_string()
}

/// Redacts the `password` value in a libpq keyword DSN (`host=… password=…`).
///
/// Handles optional spaces around `=`, single/double-quoted values (which may
/// contain spaces), and backslash escapes within a quoted value.
fn redact_keyword_password(s: &str) -> String {
    const KEY: &str = "password";
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if is_keyword_at(s, i, KEY) {
            let mut j = i + KEY.len();
            j = skip_spaces(bytes, j);
            if j < bytes.len() && bytes[j] == b'=' {
                j = skip_spaces(bytes, j + 1);
                out.push_str("password=");
                out.push_str(REDACTED);
                i = skip_value(s, j);
                continue;
            }
        }
        // Not a `password` key: copy one character and continue.
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn skip_spaces(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
        i += 1;
    }
    i
}

/// True if `keyword` starts at byte `i` and is preceded by a token boundary, so
/// that a value like `user=password` is not mistaken for a `password` key.
fn is_keyword_at(s: &str, i: usize, keyword: &str) -> bool {
    if !s[i..].starts_with(keyword) {
        return false;
    }
    i == 0 || matches!(s.as_bytes()[i - 1], b' ' | b'\t' | b'\n' | b'\r' | b';')
}

/// Returns the byte index just past a keyword value beginning at `start`.
fn skip_value(s: &str, start: usize) -> usize {
    let b = s.as_bytes();
    if start >= b.len() {
        return start;
    }
    let quote = b[start];
    if quote == b'\'' || quote == b'"' {
        let mut k = start + 1;
        while k < b.len() {
            if b[k] == b'\\' && k + 1 < b.len() {
                k += 2; // skip an escaped character (e.g. \' or \\)
                continue;
            }
            if b[k] == quote {
                return k + 1; // include the closing quote
            }
            k += 1;
        }
        return b.len(); // unterminated quote
    }
    let mut k = start;
    while k < b.len() && !matches!(b[k], b' ' | b'\t' | b'\n' | b'\r' | b'&') {
        k += 1;
    }
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_url() {
        // Userinfo password is redacted; the rest of the URL is preserved.
        let s = sanitize_connection_string("mysql://user:password123@localhost:3306/db");
        assert!(!s.contains("password123"), "{s}");
        assert!(s.contains("user:***@localhost:3306"), "{s}");

        // No password: content preserved.
        let s = sanitize_connection_string("mysql://localhost:3306/db");
        assert!(s.contains("localhost:3306"), "{s}");

        // Password containing '@' (userinfo split at the last '@').
        let s = sanitize_connection_string("mysql://user:p@ss@localhost:3306/db");
        assert!(!s.contains("p@ss"), "{s}");

        // Password as a query parameter, with a query '@' and no path slash.
        let s = sanitize_connection_string("mysql://user:p@ss@host:3306?password=secret&x=y");
        assert!(!s.contains("secret") && !s.contains("p@ss"), "{s}");

        // Query-param password with only a username in the userinfo.
        let s = sanitize_connection_string("postgresql://user@localhost:5432/mydb?password=secret");
        assert!(!s.contains("secret"), "{s}");
    }

    #[test]
    fn test_sanitize_keyword_dsn() {
        assert_eq!(
            sanitize_connection_string(
                "host=localhost port=5432 user=postgres password=secret dbname=mydb"
            ),
            "host=localhost port=5432 user=postgres password=*** dbname=mydb"
        );

        // Spaces around '='.
        let s = sanitize_connection_string("host=localhost password = secret dbname=x");
        assert!(!s.contains("secret"), "{s}");

        // Single- and double-quoted values that contain spaces.
        for dsn in [
            "host=localhost password='my secret' dbname=x",
            "host=localhost password=\"my secret\" dbname=x",
        ] {
            let s = sanitize_connection_string(dsn);
            assert_eq!(s, "host=localhost password=*** dbname=x");
        }

        // Backslash-escaped quote inside a quoted value: the whole value is
        // consumed, so no fragment leaks.
        let s = sanitize_connection_string(r"host=localhost password='pa\'ss' dbname=x");
        assert_eq!(s, "host=localhost password=*** dbname=x");

        // `password` appearing as a value (not a key) is left untouched.
        assert_eq!(
            sanitize_connection_string("host=localhost user=password dbname=x"),
            "host=localhost user=password dbname=x"
        );

        // No password: unchanged.
        assert_eq!(
            sanitize_connection_string("host=localhost user=postgres dbname=mydb"),
            "host=localhost user=postgres dbname=mydb"
        );
    }
}
