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

//! Format-neutral collection of COPY `WITH (...)` options.
//!
//! The option list is **open**: every `KEY value` entry the client writes
//! is collected here, regardless of whether any format understands it.
//! Each format codec then takes the options it knows ([`CopyOptionSet::take_string`]
//! and friends) and rejects everything else via
//! [`CopyOptionSet::reject_leftovers`]. This way a format can introduce
//! its own options — say a line-protocol format accepting
//! `WITH (FORMAT influxdb_line, precision 'ns')` — without touching the
//! statement parser or any other codec.

use pgwire::error::{PgWireError, PgWireResult};

use super::bad_copy_data;
use crate::postgres::types::PgErrorCode;

/// The value of one COPY option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CopyOptionValue {
    /// A single-quoted string literal.
    String(String),
    /// A numeric literal, kept as written.
    Number(String),
    /// A bare identifier.
    Ident(String),
    /// `TRUE` / `FALSE`.
    Boolean(bool),
    /// The option appears without a value.
    Flag,
}

impl CopyOptionValue {
    /// The value as a descriptive word for error messages.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            CopyOptionValue::String(_) => "a string value",
            CopyOptionValue::Number(_) => "a numeric value",
            CopyOptionValue::Ident(_) => "an identifier value",
            CopyOptionValue::Boolean(_) => "a boolean value",
            CopyOptionValue::Flag => "no value",
        }
    }
}

/// The options of a `COPY ... FROM STDIN WITH (...)` statement.
///
/// Entries keep statement order; keys are lowercased for lookup and
/// displayed uppercased in error messages.
#[derive(Debug, Default)]
pub(crate) struct CopyOptionSet {
    entries: Vec<(String, CopyOptionValue)>,
}

impl CopyOptionSet {
    /// Adds an option; duplicate keys are rejected like PostgreSQL does.
    pub(crate) fn insert(&mut self, key: String, value: CopyOptionValue) -> PgWireResult<()> {
        if self.entries.iter().any(|(k, _)| *k == key) {
            return Err(bad_copy_data(format!(
                "conflicting or redundant {} option",
                key.to_uppercase()
            )));
        }
        self.entries.push((key, value));
        Ok(())
    }

    /// Takes the raw value of `key`; for codec-specific options whose
    /// shape this module does not prescribe.
    pub(crate) fn take_value(&mut self, key: &str) -> Option<CopyOptionValue> {
        let key = key.to_lowercase();
        let index = self.entries.iter().position(|(k, _)| *k == key)?;
        Some(self.entries.remove(index).1)
    }

    /// Takes the `FORMAT` option, lowercased. Accepts identifier or string
    /// values.
    pub(crate) fn take_format(&mut self) -> PgWireResult<Option<String>> {
        let Some(value) = self.take_value("format") else {
            return Ok(None);
        };
        match value {
            CopyOptionValue::Ident(name) | CopyOptionValue::String(name) => {
                Ok(Some(name.to_lowercase()))
            }
            other => Err(bad_copy_data(format!(
                "FORMAT requires an identifier value, got {}",
                other.kind()
            ))),
        }
    }

    /// Takes a string-valued option.
    pub(crate) fn take_string(&mut self, key: &str) -> PgWireResult<Option<String>> {
        let Some(value) = self.take_value(key) else {
            return Ok(None);
        };
        match value {
            CopyOptionValue::String(s) => Ok(Some(s)),
            other => Err(bad_copy_data(format!(
                "COPY option {} requires a string value, got {}",
                key.to_uppercase(),
                other.kind()
            ))),
        }
    }

    /// Takes a single one-byte character option.
    pub(crate) fn take_single_byte(&mut self, key: &str) -> PgWireResult<Option<u8>> {
        let Some(value) = self.take_string(key)? else {
            return Ok(None);
        };
        let mut chars = value.chars();
        let (c, rest) = (chars.next(), chars.as_str());
        match c {
            Some(c) if rest.is_empty() && c.is_ascii() && !c.is_ascii_control() => {
                Ok(Some(c as u8))
            }
            _ => Err(bad_copy_data(format!(
                "COPY option {} must be a single one-byte non-newline character",
                key.to_uppercase()
            ))),
        }
    }

    /// Takes a boolean option; a valueless option means `TRUE`.
    pub(crate) fn take_flag(&mut self, key: &str) -> PgWireResult<Option<bool>> {
        let Some(value) = self.take_value(key) else {
            return Ok(None);
        };
        match value {
            CopyOptionValue::Flag => Ok(Some(true)),
            CopyOptionValue::Boolean(b) => Ok(Some(b)),
            other => Err(bad_copy_data(format!(
                "COPY option {} requires a boolean value, got {}",
                key.to_uppercase(),
                other.kind()
            ))),
        }
    }

    /// Rejects every option not consumed by the format, with one message
    /// naming them all. Codecs call this after their `take_*` calls so a
    /// typo cannot be silently ignored.
    pub(crate) fn reject_leftovers(self, format: &str) -> PgWireResult<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        let unknown = self
            .entries
            .iter()
            .map(|(key, _)| key.to_uppercase())
            .collect::<Vec<_>>()
            .join(", ");
        Err(PgWireError::UserError(Box::new(
            PgErrorCode::Ec0A000.to_err_info(format!(
                "COPY option(s) {unknown} are not supported by format {format}"
            )),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_set(entries: &[(&str, CopyOptionValue)]) -> CopyOptionSet {
        let mut set = CopyOptionSet::default();
        for (key, value) in entries {
            set.insert((*key).to_string(), value.clone()).unwrap();
        }
        set
    }

    #[test]
    fn test_insert_rejects_duplicates() {
        let mut set = CopyOptionSet::default();
        set.insert("format".to_string(), CopyOptionValue::Ident("csv".into()))
            .unwrap();
        let err = set
            .insert("format".to_string(), CopyOptionValue::Ident("text".into()))
            .unwrap_err();
        assert!(err.to_string().contains("redundant FORMAT"));
    }

    #[test]
    fn test_take_string_and_single_byte() {
        let mut set = new_set(&[
            ("null", CopyOptionValue::String("nil".into())),
            ("delimiter", CopyOptionValue::String("|".into())),
        ]);
        assert_eq!(set.take_string("null").unwrap().as_deref(), Some("nil"));
        assert_eq!(set.take_single_byte("delimiter").unwrap(), Some(b'|'));

        let mut set = new_set(&[("delimiter", CopyOptionValue::Number("42".into()))]);
        let err = set.take_single_byte("delimiter").unwrap_err();
        assert!(err.to_string().contains("requires a string value"));

        let mut set = new_set(&[("delimiter", CopyOptionValue::String("ab".into()))]);
        let err = set.take_single_byte("delimiter").unwrap_err();
        assert!(err.to_string().contains("single one-byte"));
    }

    #[test]
    fn test_take_flag() {
        let mut set = new_set(&[
            ("header", CopyOptionValue::Flag),
            ("frozen", CopyOptionValue::Boolean(false)),
        ]);
        assert_eq!(set.take_flag("header").unwrap(), Some(true));
        assert_eq!(set.take_flag("frozen").unwrap(), Some(false));
        assert_eq!(set.take_flag("absent").unwrap(), None);
    }

    #[test]
    fn test_take_format_normalizes() {
        let mut set = new_set(&[("format", CopyOptionValue::Ident("CSV".into()))]);
        assert_eq!(set.take_format().unwrap().as_deref(), Some("csv"));
    }

    #[test]
    fn test_reject_leftovers_lists_all() {
        let mut set = new_set(&[
            ("precision", CopyOptionValue::String("ns".into())),
            ("weird", CopyOptionValue::Flag),
        ]);
        set.take_string("precision").unwrap();
        let err = set.reject_leftovers("text").unwrap_err();
        assert!(
            err.to_string().contains("WEIRD"),
            "leftover option must be listed: {err}"
        );
        assert!(
            err.to_string().contains("not supported by format text"),
            "{err}"
        );
        // The consumed option is not listed.
        assert!(!err.to_string().contains("PRECISION"));
    }

    #[test]
    fn test_raw_value_escape_hatch() {
        let mut set = new_set(&[("precision", CopyOptionValue::Ident("ns".into()))]);
        assert_eq!(
            set.take_value("precision"),
            Some(CopyOptionValue::Ident("ns".into()))
        );
    }
}
