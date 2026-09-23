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
use std::fmt::{self, Display};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datatypes::data_type::ConcreteDataType;
use datatypes::json::JSON2_REMAINDER_FIELD_NAME;
use serde::{Deserialize, Serialize};
use snafu::{Snafu, ensure};
use store_api::storage::ColumnId;

/// Identifies a column or a typed JSON object path within a column.
///
/// # Encoding
///
/// - Column: `<column_id>` as a decimal integer, e.g. `42`.
/// - JSON path: `j:1:<column_id>:<payload>`, where `j` denotes a JSON path and
///   `1` is the encoding version. The payload is the URL-safe, unpadded Base64
///   encoding of the JSON-serialized `(path, data_type)` tuple.
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
    data_type: ConcreteDataType,
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

    /// Returns the indexed data type.
    pub fn data_type(&self) -> &ConcreteDataType {
        &self.data_type
    }
}

// TODO(fys): Replace Display with an explicit encode() method that returns
// encoding errors.
impl Display for IndexTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexTarget::ColumnId(id) => write!(f, "{}", id),
            IndexTarget::JsonPath(target) => {
                let json = serde_json::to_vec(&(&target.path, &target.data_type))
                    .map_err(|_| fmt::Error)?;
                write!(
                    f,
                    "j:1:{}:{}",
                    target.column_id,
                    URL_SAFE_NO_PAD.encode(json)
                )
            }
        }
    }
}

impl IndexTarget {
    /// Creates an index target for a JSON object path.
    pub fn new_json_path(
        column_id: ColumnId,
        path: Vec<String>,
        data_type: ConcreteDataType,
    ) -> Result<Self, TargetKeyError> {
        validate_json_path(&path)?;
        let target = JsonPathTarget {
            column_id,
            path,
            data_type,
        };
        Ok(Self::JsonPath(target))
    }

    /// Parse a target key string back into an index target description.
    pub fn decode(key: &str) -> Result<Self, TargetKeyError> {
        if let Some(json_key) = key.strip_prefix("j:1:") {
            let invalid = || InvalidJsonTargetSnafu { target_key: key }.build();
            let (column, payload) = json_key.split_once(':').ok_or_else(invalid)?;
            validate_column_key(column)?;
            let column_id = column.parse::<ColumnId>().map_err(|_| invalid())?;
            let bytes = URL_SAFE_NO_PAD.decode(payload).map_err(|_| invalid())?;
            let (path, data_type) =
                serde_json::from_slice::<(Vec<String>, ConcreteDataType)>(&bytes)
                    .map_err(|_| invalid())?;
            return Self::new_json_path(column_id, path, data_type);
        }
        validate_column_key(key)?;
        let id = key
            .parse::<ColumnId>()
            .map_err(|_| InvalidColumnIdSnafu { value: key }.build())?;
        Ok(IndexTarget::ColumnId(id))
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

fn validate_column_key(key: &str) -> Result<(), TargetKeyError> {
    ensure!(!key.is_empty(), EmptySnafu);
    ensure!(
        key.chars().all(|ch| ch.is_ascii_digit()),
        InvalidCharactersSnafu { key }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_column() {
        let target = IndexTarget::ColumnId(42);
        let key = format!("{}", target);
        assert_eq!(key, "42");
        let decoded = IndexTarget::decode(&key).unwrap();
        assert_eq!(decoded, target);
    }

    #[test]
    fn decode_rejects_empty() {
        let err = IndexTarget::decode("").unwrap_err();
        assert!(matches!(err, TargetKeyError::Empty));
    }

    #[test]
    fn decode_rejects_invalid_digits() {
        let err = IndexTarget::decode("1a2").unwrap_err();
        assert!(matches!(err, TargetKeyError::InvalidCharacters { .. }));
    }

    #[test]
    fn json_target_encoding_is_stable_and_typed() {
        let path = vec!["resource".to_string(), "service.name".to_string()];
        for (data_type, key) in [
            (
                ConcreteDataType::int32_datatype(),
                "j:1:7:W1sicmVzb3VyY2UiLCJzZXJ2aWNlLm5hbWUiXSx7IkludDMyIjp7fX1d",
            ),
            (
                ConcreteDataType::int64_datatype(),
                "j:1:7:W1sicmVzb3VyY2UiLCJzZXJ2aWNlLm5hbWUiXSx7IkludDY0Ijp7fX1d",
            ),
            (
                ConcreteDataType::string_datatype(),
                "j:1:7:W1sicmVzb3VyY2UiLCJzZXJ2aWNlLm5hbWUiXSx7IlN0cmluZyI6eyJzaXplX3R5cGUiOiJVdGY4In19XQ",
            ),
        ] {
            let target = IndexTarget::new_json_path(7, path.clone(), data_type).unwrap();
            assert_eq!(target.to_string(), key);
            assert_eq!(IndexTarget::decode(key).unwrap(), target);
            assert_eq!(
                serde_json::from_str::<IndexTarget>(&serde_json::to_string(&target).unwrap())
                    .unwrap(),
                target
            );
        }
        let target = IndexTarget::new_json_path(
            42,
            vec!["引号\".:[]".into()],
            ConcreteDataType::string_datatype(),
        )
        .unwrap();
        assert_eq!(IndexTarget::decode(&target.to_string()).unwrap(), target);
        assert_eq!(
            serde_json::to_string(&IndexTarget::ColumnId(42)).unwrap(),
            r#"{"ColumnId":42}"#
        );
    }

    #[test]
    fn json_target_serde_preserves_legacy_shape() {
        let json = r#"{"JsonPath":{"column_id":7,"path":["resource","service.name"],"data_type":{"Int64":{}}}}"#;
        let target = IndexTarget::new_json_path(
            7,
            vec!["resource".into(), "service.name".into()],
            ConcreteDataType::int64_datatype(),
        )
        .unwrap();
        assert_eq!(serde_json::to_string(&target).unwrap(), json);
        assert_eq!(serde_json::from_str::<IndexTarget>(json).unwrap(), target);
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
            let data_type = ConcreteDataType::int64_datatype();
            assert!(matches!(
                IndexTarget::new_json_path(7, path.clone(), data_type.clone()),
                Err(TargetKeyError::InvalidJsonPath { .. })
            ));
            let json = serde_json::json!({
                "column_id": 7,
                "path": path,
                "data_type": data_type,
            });
            assert!(
                serde_json::from_value::<IndexTarget>(serde_json::json!({"JsonPath": json}))
                    .is_err()
            );
            let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&(path, data_type)).unwrap());
            assert!(IndexTarget::decode(&format!("j:1:7:{payload}")).is_err());
        }
        assert!(
            serde_json::from_value::<JsonPathTarget>(serde_json::json!({
                "column_id": 7,
                "path": [],
                "data_type": ConcreteDataType::int64_datatype(),
            }))
            .is_err()
        );
        for key in ["j:2:7:e30", "j:1:7", "j:1:7:!", "j:1:x:e30", "j:1:7:e30"] {
            assert!(IndexTarget::decode(key).is_err(), "{key}");
        }
    }
}
