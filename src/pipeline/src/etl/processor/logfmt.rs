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

//! The `logfmt` processor parses a [logfmt](https://brandur.org/logfmt) line,
//! such as `level=info msg="hello world" duration=12ms`, into an object.

use std::collections::BTreeMap;

use snafu::OptionExt as _;
use vrl::prelude::Bytes;
use vrl::value::{KeyString, Value as VrlValue};

use crate::Processor;
use crate::error::{
    Error, FieldMustBeTypeSnafu, KeyMustBeStringSnafu, LogfmtParseSnafu,
    ProcessorMissingFieldSnafu, Result, ValueMustBeMapSnafu,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    FIELD_NAME, FIELDS_NAME, IGNORE_MISSING_NAME, yaml_bool, yaml_new_field, yaml_new_fields,
};

pub(crate) const PROCESSOR_LOGFMT: &str = "logfmt";

/// Parses logfmt strings into objects.
///
/// Every `key=value` pair becomes an entry of the output object with a string
/// value. A bare key without `=` (e.g. `debug` in `debug msg=hi`) is treated
/// as a flag and set to `true`. When a key appears more than once, the last
/// value wins. The output is written to the target field, or replaces the
/// input field in place when no target is given.
#[derive(Debug, Default)]
pub struct LogfmtProcessor {
    fields: Fields,
    ignore_missing: bool,
}

impl TryFrom<&yaml_rust::yaml::Hash> for LogfmtProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> std::result::Result<Self, Self::Error> {
        let mut fields = Fields::default();
        let mut ignore_missing = false;

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
            match key {
                FIELD_NAME => {
                    fields = Fields::one(yaml_new_field(v, FIELD_NAME)?);
                }
                FIELDS_NAME => {
                    fields = yaml_new_fields(v, FIELDS_NAME)?;
                }
                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }
                _ => {}
            }
        }

        Ok(LogfmtProcessor {
            fields,
            ignore_missing,
        })
    }
}

impl LogfmtProcessor {
    fn process_field(&self, val: &VrlValue) -> Result<VrlValue> {
        let Some(line) = val.as_str() else {
            return FieldMustBeTypeSnafu {
                field: val.to_string(),
                ty: "string",
            }
            .fail();
        };
        let pairs = parse_logfmt(&line).map_err(|reason| {
            LogfmtParseSnafu {
                input: line.to_string(),
                reason,
            }
            .build()
        })?;
        Ok(VrlValue::Object(pairs))
    }
}

impl Processor for LogfmtProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_LOGFMT
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, mut val: VrlValue) -> Result<VrlValue> {
        for field in self.fields.iter() {
            let index = field.input_field();
            let val = val.as_object_mut().context(ValueMustBeMapSnafu)?;
            match val.get(index) {
                Some(VrlValue::Null) | None => {
                    if !self.ignore_missing {
                        return ProcessorMissingFieldSnafu {
                            processor: self.kind(),
                            field: field.input_field(),
                        }
                        .fail();
                    }
                }
                Some(v) => {
                    let processed = self.process_field(v)?;
                    let output_index = field.target_or_input_field();
                    val.insert(KeyString::from(output_index.to_string()), processed);
                }
            }
        }
        Ok(val)
    }
}

/// Parses a logfmt line into key-value pairs.
///
/// Follows the grammar used by common logfmt implementations (e.g. go-logfmt):
/// - pairs are separated by whitespace;
/// - a key is a run of printable characters other than `=`, `"` and whitespace;
/// - a value is either unquoted (ends at the next whitespace) or double-quoted,
///   where `\"`, `\\`, `\n`, `\r`, `\t`, `\/`, `\b`, `\f` and `\uXXXX` escapes are supported;
/// - `key=` yields an empty string and a bare `key` yields `true`.
fn parse_logfmt(line: &str) -> std::result::Result<BTreeMap<KeyString, VrlValue>, String> {
    let mut output = BTreeMap::new();
    let mut chars = line.char_indices().peekable();

    loop {
        // Skip separators.
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        let Some(&(key_start, first)) = chars.peek() else {
            break;
        };
        if first == '=' || first == '"' {
            return Err(format!("unexpected '{first}' at position {key_start}"));
        }

        let mut key_end = line.len();
        while let Some(&(i, c)) = chars.peek() {
            if c == '=' || c.is_whitespace() {
                key_end = i;
                break;
            }
            if c == '"' || c.is_control() {
                return Err(format!("invalid character {c:?} in key at position {i}"));
            }
            chars.next();
        }
        let key = KeyString::from(&line[key_start..key_end]);

        if chars.next_if(|(_, c)| *c == '=').is_none() {
            // Bare key, e.g. `debug`.
            output.insert(key, VrlValue::Boolean(true));
            continue;
        }

        let value = match chars.peek() {
            Some(&(_, '"')) => {
                chars.next();
                parse_quoted_value(&mut chars)?
            }
            Some(&(value_start, _)) => {
                let mut value_end = line.len();
                while let Some(&(i, c)) = chars.peek() {
                    if c.is_whitespace() {
                        value_end = i;
                        break;
                    }
                    if c == '"' || c == '=' {
                        return Err(format!(
                            "unexpected '{c}' in unquoted value at position {i}"
                        ));
                    }
                    chars.next();
                }
                line[value_start..value_end].to_string()
            }
            None => String::new(),
        };
        output.insert(key, VrlValue::Bytes(Bytes::from(value)));
    }

    Ok(output)
}

/// Parses the rest of a double-quoted value; the opening quote is already consumed.
fn parse_quoted_value(
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
) -> std::result::Result<String, String> {
    let mut value = String::new();
    loop {
        let Some((i, c)) = chars.next() else {
            return Err("unterminated quoted value".to_string());
        };
        match c {
            '"' => {
                if let Some(&(j, next)) = chars.peek()
                    && !next.is_whitespace()
                {
                    return Err(format!(
                        "expect whitespace after quoted value at position {j}, found '{next}'"
                    ));
                }
                return Ok(value);
            }
            '\\' => {
                let Some((_, escaped)) = chars.next() else {
                    return Err("unterminated escape sequence".to_string());
                };
                match escaped {
                    '"' => value.push('"'),
                    '\\' => value.push('\\'),
                    '/' => value.push('/'),
                    'n' => value.push('\n'),
                    'r' => value.push('\r'),
                    't' => value.push('\t'),
                    'b' => value.push('\u{8}'),
                    'f' => value.push('\u{c}'),
                    'u' => {
                        let hex: String = chars.by_ref().take(4).map(|(_, c)| c).collect();
                        let decoded = (hex.len() == 4)
                            .then(|| u32::from_str_radix(&hex, 16).ok())
                            .flatten()
                            .and_then(char::from_u32)
                            .ok_or_else(|| format!("invalid unicode escape '\\u{hex}'"))?;
                        value.push(decoded);
                    }
                    other => {
                        return Err(format!("invalid escape '\\{other}' at position {i}"));
                    }
                }
            }
            c => value.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(pairs: &[(&str, VrlValue)]) -> BTreeMap<KeyString, VrlValue> {
        pairs
            .iter()
            .map(|(k, v)| (KeyString::from(*k), v.clone()))
            .collect()
    }

    fn s(v: &str) -> VrlValue {
        VrlValue::Bytes(Bytes::from(v.to_string()))
    }

    #[test]
    fn test_parse_logfmt() {
        let parsed = parse_logfmt(
            r#"ts=2026-10-14T10:00:00Z level=info msg="hello \"world\"" path=/api/v1 empty= debug"#,
        )
        .unwrap();
        assert_eq!(
            parsed,
            obj(&[
                ("ts", s("2026-10-14T10:00:00Z")),
                ("level", s("info")),
                ("msg", s(r#"hello "world""#)),
                ("path", s("/api/v1")),
                ("empty", s("")),
                ("debug", VrlValue::Boolean(true)),
            ])
        );
    }

    #[test]
    fn test_parse_logfmt_whitespace_escapes_and_duplicates() {
        let parsed = parse_logfmt("  a=1\tb=\"x\\ny\\u00e9\"  a=2  ").unwrap();
        assert_eq!(parsed, obj(&[("a", s("2")), ("b", s("x\nyé"))]));
        assert!(parse_logfmt("").unwrap().is_empty());
        assert!(parse_logfmt("   ").unwrap().is_empty());
        assert_eq!(parse_logfmt("k=\"\"").unwrap(), obj(&[("k", s(""))]));
        assert_eq!(parse_logfmt("k=").unwrap(), obj(&[("k", s(""))]));
    }

    #[test]
    fn test_parse_logfmt_invalid() {
        for input in [
            "=value",
            "\"key\"=value",
            "msg=\"unterminated",
            "msg=\"bad\\q\"",
            "msg=\"a\"b",
            "k=a\"b",
            "k=a=b",
            "msg=\"\\u12\"",
        ] {
            assert!(parse_logfmt(input).is_err(), "expect error for {input:?}");
        }
    }

    #[test]
    fn test_logfmt_processor() {
        let processor = LogfmtProcessor::default();
        let result = processor.process_field(&s("level=warn code=42")).unwrap();
        assert_eq!(
            result,
            VrlValue::Object(obj(&[("code", s("42")), ("level", s("warn"))]))
        );
        assert!(processor.process_field(&VrlValue::Integer(1)).is_err());
        assert!(processor.process_field(&s("msg=\"oops")).is_err());
    }
}
