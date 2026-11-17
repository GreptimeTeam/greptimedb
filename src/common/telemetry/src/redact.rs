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

use std::fmt;

use tracing_log::NormalizeEvent;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use url::Url;

/// Redacts passwords of connection DSNs embedded in arbitrary text such as raw
/// SQL, parser error messages, or token dumps. Text without a `scheme://` DSN is
/// returned unchanged.
pub fn redact_connection_in_text(text: &str) -> String {
    const REDACTED: &str = "******";
    let mut output = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match find_subslice(bytes, index, b"://") {
            Some(scheme_end) => {
                let mut start = scheme_end;
                while start > index && is_scheme_char(bytes[start - 1]) {
                    start -= 1;
                }
                output.push_str(&text[index..start]);

                let host_start = scheme_end + 3;
                let end = find_dsn_end(bytes, host_start);
                let raw = &text[start..end];
                // Parse the extracted DSN with the `url` crate, which splits the
                // username/password at the *first* `:` and the host at the *last*
                // `@`, so passwords containing `:`, `,`, `;`, `(`, `)` or `@` are
                // handled correctly. DSNs without a password are left unchanged.
                match Url::parse(raw)
                    .ok()
                    .filter(|url| url.password().is_some())
                    .map(|mut url| {
                        let _ = url.set_password(Some(REDACTED));
                        url.to_string()
                    }) {
                    Some(redacted) => output.push_str(&redacted),
                    None => output.push_str(raw),
                }
                index = end;
            }
            None => {
                output.push_str(&text[index..]);
                break;
            }
        }
    }
    output
}

fn is_scheme_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'+' | b'-' | b'.')
}

fn find_subslice(haystack: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack[from..]
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|pos| from + pos)
}

fn find_dsn_end(bytes: &[u8], from: usize) -> usize {
    let mut end = from;
    while end < bytes.len() {
        let c = bytes[end];
        if c.is_ascii_whitespace() || matches!(c, b'\'' | b'"' | b'<' | b'>' | b'`') {
            break;
        }
        end += 1;
    }
    // If another `scheme://` is glued directly onto this DSN (no whitespace or
    // quote separating them), stop at that scheme's start so the following DSN
    // isn't swallowed into this one's path. `//` cannot occur unencoded in a
    // userinfo/host, so a `://` after `from` is always either a new DSN or an
    // embedded URL further along in the path/query — both safe to split on.
    if let Some(next_scheme_end) = find_subslice(bytes, from, b"://") {
        let mut next_start = next_scheme_end;
        while next_start > from && is_scheme_char(bytes[next_start - 1]) {
            next_start -= 1;
        }
        if next_start < end {
            end = next_start;
        }
    }
    end
}

/// A [`FormatEvent`] wrapper that redacts connection DSNs in events emitted by
/// the `sqlparser` crate before they are written, leaving all other targets
/// untouched.
pub struct RedactingFormatter<E> {
    inner: E,
}

impl<E> RedactingFormatter<E> {
    pub fn new(inner: E) -> Self {
        Self { inner }
    }
}

impl<S, N, E> FormatEvent<S, N> for RedactingFormatter<E>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    E: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        // Events bridged from the `log` crate carry a hard-coded metadata target of
        // "log"; recover the real target (e.g. "sqlparser::parser") via
        // `NormalizeEvent::normalized_metadata`.
        let normalized_meta = event.normalized_metadata();
        let meta = normalized_meta.as_ref().unwrap_or_else(|| event.metadata());
        if !meta.target().starts_with("sqlparser") {
            return self.inner.format_event(ctx, writer, event);
        }
        let mut buf = String::new();
        self.inner.format_event(ctx, Writer::new(&mut buf), event)?;
        writer.write_str(&redact_connection_in_text(&buf))
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone)]
    struct BufWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn test_redacting_formatter_redacts_sqlparser_dsn() {
        let _ = tracing_log::LogTracer::init();

        let writer = BufWriter(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .event_format(RedactingFormatter::new(
                tracing_subscriber::fmt::format::Format::default(),
            ))
            .with_writer(writer.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            log::debug!(target: "sqlparser::parser", "CREATE EXTERNAL TABLE t ENGINE=relational WITH (connection='mysql://user:secret@127.0.0.1:3306/db')");
        });

        let output = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        assert!(!output.contains("secret"));
        assert!(output.contains("mysql://user:******@127.0.0.1:3306/db"));
    }

    #[test]
    fn test_redact_connection_in_text() {
        assert_eq!(
            redact_connection_in_text(
                "CREATE EXTERNAL TABLE t (...) ENGINE=relational \
                 WITH(connection='mysql://user:secret@127.0.0.1:3306/db', remote_table='sample')"
            ),
            "CREATE EXTERNAL TABLE t (...) ENGINE=relational \
             WITH(connection='mysql://user:******@127.0.0.1:3306/db', remote_table='sample')"
        );
        assert_eq!(
            redact_connection_in_text("Expected =, found: mysql://user:secret@127.0.0.1:3306/db"),
            "Expected =, found: mysql://user:******@127.0.0.1:3306/db"
        );
        assert_eq!(
            redact_connection_in_text("postgresql://user:secret@host:5432/db?sslmode=require"),
            "postgresql://user:******@host:5432/db?sslmode=require"
        );
        // A DSN without a password, or a non-DSN, is left unchanged.
        assert_eq!(
            redact_connection_in_text("mysql://host/db"),
            "mysql://host/db"
        );
        assert_eq!(
            redact_connection_in_text("mysql://host/db?email=a@b.com"),
            "mysql://host/db?email=a@b.com"
        );
        assert_eq!(redact_connection_in_text("SELECT 1"), "SELECT 1");
        // Multiple DSNs are all redacted.
        assert_eq!(
            redact_connection_in_text("mysql://a:one@h1/db1 and mysql://b:two@h2/db2"),
            "mysql://a:******@h1/db1 and mysql://b:******@h2/db2"
        );
        // Passwords may contain URL-legal punctuation; only the password is
        // replaced (the `url` crate splits at the first `:` and last `@`).
        assert_eq!(
            redact_connection_in_text("mysql://user:abc:def@host/db"),
            "mysql://user:******@host/db"
        );
        assert_eq!(
            redact_connection_in_text("mysql://user:abc,def@host/db"),
            "mysql://user:******@host/db"
        );
        assert_eq!(
            redact_connection_in_text("mysql://user:abc;def@host/db"),
            "mysql://user:******@host/db"
        );
        assert_eq!(
            redact_connection_in_text("mysql://user:abc(def)@host/db"),
            "mysql://user:******@host/db"
        );
        // Adjacent DSNs glued together by punctuation are still both redacted.
        assert_eq!(
            redact_connection_in_text("mysql://a:one@h1/db1,mysql://b:two@h2/db2"),
            "mysql://a:******@h1/db1,mysql://b:******@h2/db2"
        );
    }
}
