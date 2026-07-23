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

/// Substituted for a whole connection string that cannot be safely sanitized.
const REDACTED_CONNECTION_STRING: &str = "<redacted connection string>";

/// Removes sensitive information (passwords) from a connection string before it
/// is logged.
///
/// `store_addrs` entries can be a PostgreSQL DSN (a `postgres(ql)://` URL or a
/// libpq keyword string), a MySQL/etcd URL, or an etcd host list.
///
/// The function fails closed: it never returns a URI-shaped input verbatim. In
/// order:
/// 1. PostgreSQL DSNs are parsed with `tokio_postgres::Config` — the backend's
///    own parser — and logged via its `Debug`, which redacts the password. This
///    matches the grammar exactly (multi-host URIs, `\`-escapes, any Unicode
///    whitespace, percent-encoded query keys, `&`/`;`/`://` inside values).
/// 2. Any other URL is redacted with the `url` crate.
/// 3. A URI-shaped input that neither parser accepted (e.g. a malformed
///    multi-host Postgres URI, with or without leading whitespace) is redacted
///    in full — credentials in an unparsable authority or query cannot be split
///    out reliably, so the whole string is replaced rather than reparsed.
/// 4. Anything else is treated as a keyword string and best-effort redacted.
pub fn sanitize_connection_string(conn_str: &str) -> String {
    // Redact explicit keyword-style password assignments before parsing. A
    // malformed string such as `host=x;password=secret` can otherwise parse as
    // a host value, causing `Config`'s Debug output to retain the password text.
    if !is_uri_like(conn_str) {
        let redacted = redact_keyword_password(conn_str);
        if redacted != conn_str {
            return redacted;
        }
        if contains_password_assignment(conn_str) {
            return REDACTED_CONNECTION_STRING.to_string();
        }
    }

    #[cfg(feature = "pg_kvbackend")]
    if let Ok(config) = conn_str.parse::<tokio_postgres::Config>() {
        // `Config`'s Debug prints the password as `Some("REDACTED")`.
        return format!("{config:?}");
    }

    if let Ok(url) = Url::parse(conn_str) {
        return redact_url(url);
    }

    if is_uri_like(conn_str) {
        return REDACTED_CONNECTION_STRING.to_string();
    }

    redact_keyword_password(conn_str)
}

/// Redacts the userinfo password and any `password` query parameter of a parsed URL.
fn redact_url(mut url: Url) -> String {
    if url.password().is_some() {
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

/// True if `s`, ignoring leading whitespace, begins with a valid URI scheme
/// followed by `://` (`scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )`).
/// Leading whitespace (including Unicode, e.g. U+2003) does not stop a value from
/// being a URI, so it is trimmed first; a bare `contains("://")` would instead
/// misfire on a `://` inside a keyword value.
fn is_uri_like(s: &str) -> bool {
    let s = s.trim_start();
    let Some(pos) = s.find("://") else {
        return false;
    };
    let mut chars = s[..pos].chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

/// Best-effort redaction of a `password` value in a keyword string, used only
/// for inputs that neither `tokio_postgres::Config` nor [`Url`] accept (e.g. a
/// libpq keyword DSN when `pg_kvbackend` is disabled). Keys are separated by
/// Unicode whitespace; a value is single/double quoted or runs to the next
/// unescaped whitespace, with `\` escaping the next character.
fn redact_keyword_password(s: &str) -> String {
    const KEY: &str = "password";
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if is_keyword_at(s, i, KEY) {
            let mut j = skip_ws(s, i + KEY.len());
            if s[j..].starts_with('=') {
                j = skip_ws(s, j + 1);
                out.push_str("password=");
                out.push_str(REDACTED);
                i = skip_value(s, j);
                continue;
            }
        }
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Returns true if `s` contains a `password` assignment that is not embedded in
/// an identifier. This is a fail-closed fallback for malformed keyword strings
/// where the backend parser may otherwise treat the assignment as another
/// option's value.
fn contains_password_assignment(s: &str) -> bool {
    const KEY: &str = "password";
    s.match_indices(KEY).any(|(i, _)| {
        let embedded_in_identifier = s[..i]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_alphanumeric() || c == '_');
        !embedded_in_identifier && s[skip_ws(s, i + KEY.len())..].starts_with('=')
    })
}

fn skip_ws(s: &str, mut i: usize) -> usize {
    while let Some(ch) = s[i..].chars().next() {
        if ch.is_whitespace() {
            i += ch.len_utf8();
        } else {
            break;
        }
    }
    i
}

/// True if `keyword` starts at byte `i` preceded by a keyword boundary. In
/// addition to valid libpq whitespace separators, `;` and `&` are treated as
/// conservative boundaries so malformed DSNs cannot leak credentials. `=` is
/// deliberately excluded, so a value like `application_name=password=x` is not
/// mistaken for a password key.
fn is_keyword_at(s: &str, i: usize, keyword: &str) -> bool {
    s[i..].starts_with(keyword)
        && (i == 0
            || s[..i]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_whitespace() || matches!(c, ';' | '&')))
}

/// Returns the byte index just past a keyword value beginning at `start`.
fn skip_value(s: &str, start: usize) -> usize {
    if start >= s.len() {
        return start;
    }
    let quote = s[start..].chars().next().unwrap();
    let quoted = quote == '\'' || quote == '"';
    let mut i = if quoted {
        start + quote.len_utf8()
    } else {
        start
    };
    while i < s.len() {
        let ch = s[i..].chars().next().unwrap();
        if ch == '\\' {
            i += ch.len_utf8();
            if let Some(next) = s[i..].chars().next() {
                i += next.len_utf8(); // skip the escaped character
            }
            continue;
        }
        if quoted {
            if ch == quote {
                return i + ch.len_utf8(); // include the closing quote
            }
        } else if ch.is_whitespace() {
            return i;
        }
        i += ch.len_utf8();
    }
    s.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_non_pg_url() {
        let s = sanitize_connection_string("mysql://user:password123@localhost:3306/db");
        assert!(!s.contains("password123"), "{s}");
        assert!(s.contains("user:***@localhost:3306"), "{s}");

        let s = sanitize_connection_string("mysql://localhost:3306/db");
        assert!(s.contains("localhost:3306"), "{s}");

        let s = sanitize_connection_string("mysql://user@localhost:3306/db?password=secret");
        assert!(!s.contains("secret"), "{s}");

        let s = sanitize_connection_string("http://etcd-host:2379");
        assert!(s.contains("etcd-host:2379"), "{s}");
    }

    // Fail-closed: a URI-shaped string that neither parser accepts is redacted in
    // full. These multi-host URIs are rejected by both `url` (InvalidPort) and,
    // when compiled, `tokio_postgres::Config` (unknown option), so the invariant
    // holds regardless of the `pg_kvbackend` feature — including with leading
    // (Unicode) whitespace and percent-encoded query keys.
    #[test]
    fn test_sanitize_fail_closed_uri() {
        for dsn in [
            "postgresql://user:secret@host1:1234,host2:5678/db?unknown=x",
            " postgresql://user:secret@host1:1234,host2:5678/db?unknown=x",
            "\u{2003}postgresql://user:secret@host1:1234,host2:5678/db?unknown=x",
            "postgresql://user@host1:1,host2:2/db?pass%77ord=qsecret&unknown=x",
        ] {
            let s = sanitize_connection_string(dsn);
            // "secret" also matches "qsecret", so this covers the query case too.
            assert!(!s.contains("secret"), "leaked for {dsn:?}: {s}");
            // Redacted whole, not reparsed: the authority is gone as well.
            assert!(!s.contains("host1"), "not fully redacted for {dsn:?}: {s}");
        }
    }

    #[test]
    fn test_sanitize_malformed_keyword_fail_closed() {
        for dsn in [
            "host=localhost;password=LEAK_CANARY",
            "host=localhost&password=LEAK_CANARY",
        ] {
            let s = sanitize_connection_string(dsn);
            assert!(!s.contains("LEAK_CANARY"), "leaked for {dsn:?}: {s}");
        }

        for dsn in [
            "host=localhost,password=LEAK_CANARY",
            "host=localhost/password=LEAK_CANARY",
            "host=localhost?password=LEAK_CANARY",
            "host=localhost#password=LEAK_CANARY",
            "host=localhost:password=LEAK_CANARY",
            "application_name=password=LEAK_CANARY",
        ] {
            let s = sanitize_connection_string(dsn);
            assert_eq!(REDACTED_CONNECTION_STRING, s, "for {dsn:?}");
        }

        let s = sanitize_connection_string("notpassword=LEAK_CANARY");
        assert!(s.contains("LEAK_CANARY"), "over-redacted: {s}");
    }

    // Keyword redaction must handle libpq quoting and Unicode whitespace without
    // misfiring on `=`/`://` inside values. Exact output is asserted without the
    // pg feature; feature-enabled coverage below asserts the no-leak invariant.
    #[cfg(not(feature = "pg_kvbackend"))]
    #[test]
    fn test_sanitize_keyword() {
        for (dsn, ok) in [
            (
                "host=localhost password=secret dbname=x",
                "host=localhost password=*** dbname=x",
            ),
            (
                "host=localhost password = secret dbname=x",
                "host=localhost password=*** dbname=x",
            ),
            (
                "host=localhost password='my secret' dbname=x",
                "host=localhost password=*** dbname=x",
            ),
            (
                "host=localhost password=\"my secret\" dbname=x",
                "host=localhost password=*** dbname=x",
            ),
            (
                r"host=localhost password='pa\'ss' dbname=x",
                "host=localhost password=*** dbname=x",
            ),
            (
                "host=localhost user=password dbname=x",
                "host=localhost user=password dbname=x",
            ),
            // A value that merely contains `://` is not treated as a URI.
            (
                "host=localhost password=secret application_name=svc://a",
                "host=localhost password=*** application_name=svc://a",
            ),
            // `&` and a `\`-escaped space are part of an unquoted libpq value, so
            // the whole value is consumed (no trailing suffix leaks).
            (
                "host=localhost password=secret&suffix dbname=x",
                "host=localhost password=*** dbname=x",
            ),
            (
                r"host=localhost password=secret\ suffix dbname=x",
                "host=localhost password=*** dbname=x",
            ),
            // `;` is not a libpq delimiter, so it belongs to the value too.
            ("password=secret;host=localhost", "password=***"),
        ] {
            assert_eq!(sanitize_connection_string(dsn), ok, "for {dsn:?}");
        }

        // Unicode whitespace as separator / around '=' must not leak.
        for dsn in [
            "host=localhost\u{2003}password=secret",
            "host=localhost password\u{2003}=\u{2003}secret",
        ] {
            assert!(
                !sanitize_connection_string(dsn).contains("secret"),
                "leaked for {dsn:?}"
            );
        }
    }

    // With the pg backend enabled, cover both the conservative keyword pre-pass
    // and URI forms parsed by tokio-postgres.
    #[cfg(feature = "pg_kvbackend")]
    #[test]
    fn test_sanitize_pg_dsn() {
        for dsn in [
            "postgresql://user:secret@localhost:5432/db",
            "postgresql://user:secret@host1:1234,host2,host3:5678/db",
            "postgresql://user@host1:1,host2/db?pass%77ord=secret",
            "host=localhost port=5432 user=postgres password=secret dbname=mydb",
            "host=localhost\u{2003}password=secret",
            "password=secret;host=localhost",
            "host=localhost password=secret application_name=http://client",
        ] {
            assert!(
                !sanitize_connection_string(dsn).contains("secret"),
                "leaked for {dsn:?}"
            );
        }
    }
}
