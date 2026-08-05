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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::extension::{
    EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType,
};
use arrow_schema::{ArrowError, DataType, Field};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::json::JsonSettings;

const LEGACY_JSON_STRUCTURE_SETTINGS_KEY: &str = "json_structure_settings";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JsonMetadata {
    /// JSON2 settings stored in Arrow extension metadata.
    json_settings: JsonSettings,
}

impl JsonMetadata {
    /// Creates JSON2 extension metadata.
    pub fn new(json_settings: JsonSettings) -> Self {
        Self { json_settings }
    }

    /// Returns the JSON2 settings.
    pub fn json_settings(&self) -> &JsonSettings {
        &self.json_settings
    }
}

/// Arrow extension type for legacy JSONB columns.
#[derive(Debug, Clone, Default)]
pub struct JsonExtensionType;

impl ExtensionType for JsonExtensionType {
    const NAME: &'static str = "greptime.json";
    type Metadata = ();

    fn metadata(&self) -> &Self::Metadata {
        &()
    }

    fn serialize_metadata(&self) -> Option<String> {
        None
    }

    fn deserialize_metadata(_metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Ok(())
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            DataType::Binary | DataType::Null => Ok(()),
            t => Err(ArrowError::InvalidArgumentError(format!(
                "Unexpected data type {t} for JsonExtensionType"
            ))),
        }
    }

    fn try_new(data_type: &DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        Self.supports_data_type(data_type).map(|_| Self)
    }
}

/// Arrow extension type for JSON2 columns and concretized projections.
#[derive(Debug, Clone, Default)]
pub struct Json2ExtensionType(Arc<JsonMetadata>);

impl Json2ExtensionType {
    /// Creates a JSON2 extension type with the given metadata.
    pub fn new(metadata: Arc<JsonMetadata>) -> Self {
        Self(metadata)
    }
}

impl ExtensionType for Json2ExtensionType {
    const NAME: &'static str = "greptime.json2";
    type Metadata = Arc<JsonMetadata>;

    fn metadata(&self) -> &Self::Metadata {
        &self.0
    }

    fn serialize_metadata(&self) -> Option<String> {
        serde_json::to_string(self.metadata()).ok()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        if let Some(metadata) = metadata {
            let metadata = serde_json::from_str(metadata).map_err(|e| {
                ArrowError::ParseError(format!("Failed to deserialize JSON metadata: {}", e))
            })?;
            Ok(Arc::new(metadata))
        } else {
            Ok(Arc::new(JsonMetadata::default()))
        }
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            // object
            DataType::Struct(_)
            // array
            | DataType::List(_)
            | DataType::ListView(_)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            // string
            | DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8
            // number
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            // boolean
            | DataType::Boolean
            // null
            | DataType::Null
            // legacy json type
            | DataType::Binary => Ok(()),
            dt => Err(ArrowError::SchemaError(format!(
                "Unexpected data type {dt}"
            ))),
        }
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let json = Self(metadata);
        json.supports_data_type(data_type)?;
        Ok(json)
    }
}

/// Checks whether this field is either a legacy JSONB or JSON2 extension type.
pub fn is_any_json_extension_type<T: AsRef<Field>>(field: T) -> bool {
    let name = field.as_ref().extension_type_name();
    name == Some(JsonExtensionType::NAME) || name == Some(Json2ExtensionType::NAME)
}

/// Parses JSON2 settings stored by the historical `greptime.json` extension.
pub fn parse_legacy_json2_settings(
    metadata: &HashMap<String, String>,
) -> crate::error::Result<Option<JsonSettings>> {
    #[derive(Deserialize)]
    struct LegacyJsonMetadata {
        #[serde(default)]
        json_settings: Option<JsonSettings>,
    }

    if metadata.get(EXTENSION_TYPE_NAME_KEY).map(String::as_str) != Some(JsonExtensionType::NAME) {
        return Ok(None);
    }

    metadata
        .get(EXTENSION_TYPE_METADATA_KEY)
        .map(|json| {
            serde_json::from_str::<LegacyJsonMetadata>(json)
                .map(|x| x.json_settings)
                .context(crate::error::DeserializeSnafu { json })
        })
        .transpose()
        .map(Option::flatten)
}

/// Checks whether this field uses the JSON2 extension layout from before type hints.
///
/// That layout used the same `greptime.json` extension name and
/// `json_structure_settings` metadata as legacy JSONB. Its structured Arrow data type is
/// therefore required to distinguish JSON2 from Binary JSONB.
pub fn is_legacy_json2_extension_type<T: AsRef<Field>>(field: T) -> bool {
    let field = field.as_ref();
    if field.extension_type_name() != Some(JsonExtensionType::NAME)
        || !matches!(field.data_type(), DataType::Struct(_))
    {
        return false;
    }

    field
        .metadata()
        .get(EXTENSION_TYPE_METADATA_KEY)
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .is_some_and(|metadata| metadata.get(LEGACY_JSON_STRUCTURE_SETTINGS_KEY).is_some())
}

/// Check if this field is a JSON2 extension type.
///
/// New schemas use [`Json2ExtensionType`]. For compatibility, old fields using
/// [`JsonExtensionType`] with JSON settings or the pre-type-hint structured layout are also
/// recognized as JSON2.
pub fn is_json2_extension_type<T: AsRef<Field>>(field: T) -> bool {
    let field = field.as_ref();
    field.extension_type_name() == Some(Json2ExtensionType::NAME)
        || parse_legacy_json2_settings(field.metadata()).is_ok_and(|x| x.is_some())
        || is_legacy_json2_extension_type(field)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
    use arrow_schema::{Field, Fields};

    use super::*;

    #[test]
    fn test_json2_extension_type_detection() {
        let extension = Json2ExtensionType::new(Arc::new(JsonMetadata::default()));
        let json2 = Field::new("j", DataType::Struct(Fields::empty()), true)
            .with_extension_type(extension.clone());
        // "projection" is the special hack for selecting the whole column of json2
        let projection = Field::new("j", DataType::Binary, true).with_extension_type(extension);
        let legacy_json2 = Field::new("j", DataType::Struct(Fields::empty()), true).with_metadata(
            HashMap::from([
                (
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    JsonExtensionType::NAME.to_string(),
                ),
                (
                    EXTENSION_TYPE_METADATA_KEY.to_string(),
                    serde_json::json!({ "json_settings": JsonSettings::default() }).to_string(),
                ),
            ]),
        );
        // Before type hints, JSON2 and JSONB shared extension metadata and were distinguished by
        // their physical Arrow data types.
        let legacy_structure_metadata = HashMap::from([
            (
                EXTENSION_TYPE_NAME_KEY.to_string(),
                JsonExtensionType::NAME.to_string(),
            ),
            (
                EXTENSION_TYPE_METADATA_KEY.to_string(),
                serde_json::json!({
                    (LEGACY_JSON_STRUCTURE_SETTINGS_KEY): { "Structured": null }
                })
                .to_string(),
            ),
        ]);
        let pre_type_hint_json2 = Field::new("j", DataType::Struct(Fields::empty()), true)
            .with_metadata(legacy_structure_metadata.clone());
        let legacy_jsonb =
            Field::new("j", DataType::Binary, true).with_metadata(legacy_structure_metadata);

        assert!(is_json2_extension_type(&json2));
        assert!(is_json2_extension_type(&projection));
        assert!(is_json2_extension_type(&legacy_json2));
        assert!(is_legacy_json2_extension_type(&pre_type_hint_json2));
        assert!(is_json2_extension_type(&pre_type_hint_json2));
        assert_eq!(
            Some(JsonSettings::default()),
            parse_legacy_json2_settings(legacy_json2.metadata()).unwrap()
        );
        assert!(!is_legacy_json2_extension_type(&legacy_jsonb));
        assert!(!is_json2_extension_type(&legacy_jsonb));
        assert!(JsonExtensionType::try_new(&DataType::Binary, ()).is_ok());
        assert!(JsonExtensionType::try_new(&DataType::Null, ()).is_ok());
        assert!(JsonExtensionType::try_new(&DataType::Struct(Fields::empty()), ()).is_err());
    }
}
