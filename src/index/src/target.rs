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

use std::any::Any;

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datatypes::json::JSON2_REMAINDER_FIELD_NAME;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu, ensure};
use store_api::storage::ColumnId;

/// Identifies a column or a JSON object path within a column.
///
/// # Encoding
///
/// - Columns are encoded as decimal IDs, e.g. `1`.
/// - JSON path targets use `json-{column_id}:` followed by path segments encoded
///   as `{UTF-8 byte length}:{segment}`, e.g. `json-1:7:service4:name`.
///   IDs and lengths are unsigned decimal integers without leading zeros.
///   Segments are read by byte length, so their contents need no escaping.
///
/// This persisted encoding is independent of the serde representation of these types.
///
/// Note: Columns retain decimal ID encoding because existing index files use
/// these IDs as lookup keys. Changing the encoding would prevent queries from
/// finding those indexes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum IndexTarget {
    ColumnId(ColumnId),
    JsonPath(JsonPathTarget),
}

/// An index target for a JSON object path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JsonPathTarget {
    column_id: ColumnId,
    #[serde(deserialize_with = "deserialize_json_path")]
    path: Vec<String>,
}

impl JsonPathTarget {
    /// Returns the ID of the root column containing this JSON path.
    pub fn column_id(&self) -> ColumnId {
        self.column_id
    }

    /// Returns the JSON object path.
    pub fn path(&self) -> &[String] {
        &self.path
    }
}

impl IndexTarget {
    /// Estimates the memory used by this target, including JSON path strings.
    pub fn approx_mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::ColumnId(_) => 0,
                Self::JsonPath(target) => target
                    .path
                    .iter()
                    .map(|part| std::mem::size_of::<String>() + part.len())
                    .sum::<usize>(),
            }
    }

    /// Creates an index target for a JSON object path.
    pub fn new_json_path(column_id: ColumnId, path: Vec<String>) -> Result<Self, TargetKeyError> {
        validate_json_path(&path)?;
        let target = JsonPathTarget { column_id, path };
        Ok(Self::JsonPath(target))
    }

    /// Encodes this target using the format described in [`IndexTarget`].
    pub fn encode(&self) -> String {
        let target = match self {
            Self::ColumnId(id) => return id.to_string(),
            Self::JsonPath(target) => target,
        };
        let mut key = format!("json-{}:", target.column_id);
        for part in &target.path {
            key.push_str(&part.len().to_string());
            key.push(':');
            key.push_str(part);
        }
        key
    }

    /// Decodes an index name using the format described in [`IndexTarget`].
    pub fn decode(key: &str) -> Result<Self, TargetKeyError> {
        ensure!(!key.is_empty(), EmptySnafu);
        if key.bytes().all(|byte| byte.is_ascii_digit()) {
            let id = key
                .parse::<ColumnId>()
                .map_err(|_| InvalidColumnIdSnafu { value: key }.build())?;
            return Ok(Self::ColumnId(id));
        }

        let invalid_key = || InvalidJsonTargetSnafu { target_key: key };
        // JSON target IDs and lengths use decimal digits without signs or leading zeros.
        let is_canonical_decimal = |value: &str| {
            !value.is_empty()
                && value.bytes().all(|byte| byte.is_ascii_digit())
                && (value.len() == 1 || !value.starts_with('0'))
        };

        // The prefix contains the root column ID; the remainder contains path segments.
        let (column_id, mut rest) = key
            .strip_prefix("json-")
            .and_then(|rest| rest.split_once(':'))
            .with_context(invalid_key)?;
        ensure!(is_canonical_decimal(column_id), invalid_key());
        let column_id = column_id.parse().ok().with_context(invalid_key)?;

        let mut path = Vec::new();
        while !rest.is_empty() {
            let (length, content) = rest.split_once(':').with_context(invalid_key)?;
            ensure!(is_canonical_decimal(length), invalid_key());
            let length = length.parse::<usize>().ok().with_context(invalid_key)?;
            // Read exactly this many bytes, including any delimiters in the segment.
            // Check bounds and the UTF-8 boundary before allocating the segment string.
            let (part, remaining) = content.split_at_checked(length).with_context(invalid_key)?;
            path.push(part.to_string());
            rest = remaining;
        }
        Self::new_json_path(column_id, path)
    }
}

/// Errors that can occur when working with index target keys.
#[derive(Snafu, Clone, PartialEq, Eq)]
#[stack_trace_debug]
pub enum TargetKeyError {
    #[snafu(display("target key cannot be empty"))]
    Empty,

    #[snafu(display("target key must contain digits only: {key}"))]
    InvalidCharacters { key: String },

    #[snafu(display("failed to parse column id from '{value}'"))]
    InvalidColumnId { value: String },

    #[snafu(display("invalid JSON index target key: {target_key}"))]
    InvalidJsonTarget { target_key: String },

    #[snafu(display("invalid JSON index path: {detail}"))]
    InvalidJsonPath { detail: String },
}

fn deserialize_json_path<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let path = Vec::<String>::deserialize(deserializer)?;
    validate_json_path(&path).map_err(serde::de::Error::custom)?;
    Ok(path)
}

fn validate_json_path(path: &[String]) -> Result<(), TargetKeyError> {
    ensure!(
        !path.is_empty(),
        InvalidJsonPathSnafu {
            detail: "path must not be empty",
        }
    );
    for (index, part) in path.iter().enumerate() {
        ensure!(
            !part.is_empty(),
            InvalidJsonPathSnafu {
                detail: format!("path segment at index {index} must not be empty"),
            }
        );
        ensure!(
            part != JSON2_REMAINDER_FIELD_NAME,
            InvalidJsonPathSnafu {
                detail: format!("path segment at index {index} uses reserved name '{part}'"),
            }
        );
    }
    Ok(())
}

impl ErrorExt for TargetKeyError {
    fn status_code(&self) -> StatusCode {
        StatusCode::InvalidArguments
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_column() {
        let target = IndexTarget::ColumnId(42);
        let key = target.encode();
        assert_eq!(key, "42");
        assert_eq!(IndexTarget::decode("42").unwrap(), target);
        let decoded = IndexTarget::decode(&key).unwrap();
        assert_eq!(decoded, target);
    }

    #[test]
    fn encode_decode_column_id_bounds() {
        for id in [0, 1, ColumnId::MAX] {
            let target = IndexTarget::ColumnId(id);
            let key = id.to_string();
            assert_eq!(target.encode(), key);
            assert_eq!(IndexTarget::decode(&key).unwrap(), target);
        }
        for key in ["-1", "4294967296", "1.5", "\"1\"", "null"] {
            assert!(IndexTarget::decode(key).is_err(), "{key}");
        }
    }

    #[test]
    fn decode_rejects_empty() {
        let err = IndexTarget::decode("").unwrap_err();
        assert!(matches!(err, TargetKeyError::Empty));
    }

    #[test]
    fn decode_rejects_invalid_digits() {
        let err = IndexTarget::decode("1a2").unwrap_err();
        assert!(matches!(err, TargetKeyError::InvalidJsonTarget { .. }));
    }

    #[test]
    fn json_target_encoding_is_stable() {
        let target =
            IndexTarget::new_json_path(7, vec!["resource".into(), "service.name".into()]).unwrap();
        let key = "json-7:8:resource12:service.name";
        assert_eq!(target.encode(), key);
        assert_eq!(IndexTarget::decode(key).unwrap(), target);
        let json = r#"{"JsonPath":{"column_id":7,"path":["resource","service.name"]}}"#;
        assert_eq!(serde_json::to_string(&target).unwrap(), json);
        assert_eq!(serde_json::from_str::<IndexTarget>(json).unwrap(), target);
        let target = IndexTarget::new_json_path(42, vec!["引号\".:[]".into()]).unwrap();
        assert_eq!(IndexTarget::decode(&target.encode()).unwrap(), target);
        assert_eq!(
            serde_json::to_string(&IndexTarget::ColumnId(42)).unwrap(),
            r#"{"ColumnId":42}"#
        );
    }

    #[test]
    fn json_target_encode_decode_segments() {
        let cases: &[(ColumnId, &[&str], &str)] = &[
            (42, &["service", "name"], "json-42:7:service4:name"),
            (0, &["a"], "json-0:1:a"),
            (ColumnId::MAX, &["a"], "json-4294967295:1:a"),
            (42, &["a.b"], "json-42:3:a.b"),
            (42, &["a", "b"], "json-42:1:a1:b"),
            (42, &["a:b", "12:xyz"], "json-42:3:a:b6:12:xyz"),
            (42, &["中文", "😀"], "json-42:6:中文4:😀"),
            (42, &["\"\\\n\0"], "json-42:4:\"\\\n\0"),
            (42, &["abcdefghijkl"], "json-42:12:abcdefghijkl"),
        ];
        for &(column_id, path, key) in cases {
            let target = IndexTarget::new_json_path(
                column_id,
                path.iter().map(|part| (*part).to_string()).collect(),
            )
            .unwrap();
            assert_eq!(target.encode(), key);
            assert_eq!(IndexTarget::decode(key).unwrap(), target);
        }
    }

    #[test]
    fn json_target_rejects_malformed_framing() {
        for key in [
            "json-",
            "json-42",
            "json-:1:a",
            "json-+42:1:a",
            "json-042:1:a",
            "json-4294967296:1:a",
            "json-42:",
            "json-42:0:",
            "json-42:1:a0:",
            "json-42::a",
            "json-42:+1:a",
            "json-42:-1:a",
            "json-42:01:a",
            "json-42:1",
            "json-42:2:a",
            "json-42:1:中",
            "json-42:3:中x",
            "json-42:1:a1:",
            "json-42:999999999999999999999999999999999999:a",
        ] {
            assert!(IndexTarget::decode(key).is_err(), "{key}");
        }
        for path in [
            vec![JSON2_REMAINDER_FIELD_NAME],
            vec!["nested", JSON2_REMAINDER_FIELD_NAME],
        ] {
            let mut key = "json-42:".to_string();
            for part in path {
                key.push_str(&format!("{}:{part}", part.len()));
            }
            assert!(matches!(
                IndexTarget::decode(&key),
                Err(TargetKeyError::InvalidJsonPath { .. })
            ));
        }
    }

    #[test]
    fn json_target_rejects_invalid_paths_and_keys() {
        for path in [
            vec![],
            vec![String::new()],
            vec!["a".into(), String::new()],
            vec![JSON2_REMAINDER_FIELD_NAME.into()],
            vec!["a".into(), JSON2_REMAINDER_FIELD_NAME.into()],
            vec!["a".into(), JSON2_REMAINDER_FIELD_NAME.into(), "b".into()],
        ] {
            assert!(matches!(
                IndexTarget::new_json_path(7, path.clone()),
                Err(TargetKeyError::InvalidJsonPath { .. })
            ));
            let json = serde_json::json!({
                "column_id": 7,
                "path": path,
            });
            assert!(
                serde_json::from_value::<IndexTarget>(serde_json::json!({"JsonPath": json}))
                    .is_err()
            );
            let key = serde_json::json!({"JsonPath": {"column_id": 7, "path": path}}).to_string();
            assert!(IndexTarget::decode(&key).is_err());
        }
        assert!(
            serde_json::from_value::<JsonPathTarget>(serde_json::json!({
                "column_id": 7,
                "path": [],
            }))
            .is_err()
        );
        for key in ["j.2.7.e30", "j.1.7", "j.1.7.!", "j.1.x.e30", "j.1.7.e30"] {
            assert!(IndexTarget::decode(key).is_err(), "{key}");
        }
    }
}
