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

/// Redacts passwords of connection DSNs embedded in arbitrary text such as raw
/// SQL, parser error messages, or token dumps. Text without a `scheme://` DSN is
/// returned unchanged.
pub fn redact_connection_in_text(text: &str) -> String {
    const REDACTED: &str = "REDACTED";
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
                let end = find_connection_end(bytes, host_start);
                let authority_end = bytes[host_start..end]
                    .iter()
                    .position(|&b| matches!(b, b'/' | b'?' | b'#'))
                    .map(|p| host_start + p)
                    .unwrap_or(end);
                let at = bytes[host_start..authority_end]
                    .iter()
                    .rposition(|&b| b == b'@')
                    .map(|p| host_start + p);

                match at.and_then(|at| {
                    bytes[host_start..at]
                        .iter()
                        .rposition(|&b| b == b':')
                        .map(|colon| (at, host_start + colon))
                }) {
                    Some((at, colon)) => {
                        output.push_str(&text[start..=colon]);
                        output.push_str(REDACTED);
                        output.push_str(&text[at..end]);
                    }
                    None => output.push_str(&text[start..end]),
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

fn find_connection_end(bytes: &[u8], from: usize) -> usize {
    let mut end = from;
    while end < bytes.len() {
        let c = bytes[end];
        if c.is_ascii_whitespace()
            || matches!(
                c,
                b'\'' | b'"' | b',' | b';' | b'(' | b')' | b'<' | b'>' | b'`'
            )
        {
            break;
        }
        end += 1;
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
