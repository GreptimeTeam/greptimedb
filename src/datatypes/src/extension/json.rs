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
use arrow_schema::{ArrowError, DataType, Field, FieldRef};
use parquet_variant_compute::VariantType;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::error::{InvalidJson2LayoutSnafu, Result as DatatypesResult};
use crate::json::JsonSettings;

const LEGACY_JSON_STRUCTURE_SETTINGS_KEY: &str = "json_structure_settings";

/// Legacy JSON2 storage layout with unlimited path expansion.
const JSON2_LAYOUT_V1: u8 = 1;
/// JSON2 storage layout with bounded path expansion and a Variant remainder.
const JSON2_LAYOUT_V2: u8 = 2;
/// Reserved physical field containing unexpanded JSON2 paths.
pub const JSON2_REMAINDER_FIELD_NAME: &str = "!__remainder__!";

/// Parsed physical layout of a JSON2 Arrow root field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Json2PhysicalLayout {
    version: u8,
}

impl Json2PhysicalLayout {
    /// Parses the JSON2 layout version from a root field.
    pub fn try_from_root(field: &Field) -> DatatypesResult<Self> {
        // A physical Struct alone cannot identify JSON2 because ordinary Struct columns use the
        // same Arrow shape. Require the extension marker so callers cannot accidentally route
        // unrelated Binary children through the legacy JSON2 Variant path.
        if !is_json2_extension_type(field) {
            return InvalidJson2LayoutSnafu {
                reason: format!("field '{}' is not a JSON2 extension", field.name()),
            }
            .fail();
        }

        let version = match field.extension_type_name() {
            Some(Json2ExtensionType::NAME) => field
                .metadata()
                .get(EXTENSION_TYPE_METADATA_KEY)
                .map(|x| parse_version(x))
                .transpose()?
                .flatten()
                .unwrap_or(JSON2_LAYOUT_V1),
            _ => JSON2_LAYOUT_V1,
        };
        Ok(Self { version })
    }

    /// Returns whether this is the bounded JSON2 layout.
    pub fn is_version_2(&self) -> bool {
        self.version == JSON2_LAYOUT_V2
    }
}

fn parse_version(metadata: &str) -> DatatypesResult<Option<u8>> {
    serde_json::from_str::<JsonMetadata>(metadata)
        .map(|x| x.layout_version)
        .map_err(|e| {
            InvalidJson2LayoutSnafu {
                reason: format!(
                    r#"invalid extension metadata: "{}", error: {}"#,
                    metadata, e,
                ),
            }
            .build()
        })
}

/// Returns the "remainder" field of a JSON2 v2 root.
pub fn json2_remainder_field(field: &Field) -> DatatypesResult<&FieldRef> {
    // The reserved name existed in some legacy shapes, so the child name alone cannot prove that
    // it has v2 Variant semantics. Validate the root before exposing it as a v2 remainder.
    if !Json2PhysicalLayout::try_from_root(field)?.is_version_2() {
        return InvalidJson2LayoutSnafu {
            reason: format!("JSON2 root '{}' is not layout v2", field.name()),
        }
        .fail();
    }

    let DataType::Struct(fields) = field.data_type() else {
        return InvalidJson2LayoutSnafu {
            reason: format!(
                "expecting the Struct datatype, actual: '{}'",
                field.data_type(),
            ),
        }
        .fail();
    };
    let remainder = fields
        .iter()
        .find(|x| x.name() == JSON2_REMAINDER_FIELD_NAME)
        .context(InvalidJson2LayoutSnafu {
            reason: "missing the 'remainder' field",
        })?;
    remainder.try_extension_type::<VariantType>().map_err(|e| {
        InvalidJson2LayoutSnafu {
            reason: e.to_string(),
        }
        .build()
    })?;
    Ok(remainder)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonMetadata {
    /// JSON2 settings stored in Arrow extension metadata.
    json_settings: JsonSettings,
    /// Physical JSON2 layout used by this Arrow field.
    ///
    /// Missing metadata denotes the legacy v1 layout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    layout_version: Option<u8>,
}

impl JsonMetadata {
    /// Creates metadata for the bounded JSON2 layout.
    pub fn new(json_settings: JsonSettings) -> Self {
        Self {
            json_settings,
            layout_version: Some(JSON2_LAYOUT_V2),
        }
    }

    /// Creates metadata for the legacy JSON2 layout.
    pub fn new_v1(json_settings: JsonSettings) -> Self {
        Self {
            json_settings,
            layout_version: None,
        }
    }

    /// Returns the JSON2 settings.
    pub fn json_settings(&self) -> &JsonSettings {
        &self.json_settings
    }

    /// Returns whether this metadata describes the JSON2 layout version 2.
    pub fn is_version_2(&self) -> bool {
        self.layout_version == Some(JSON2_LAYOUT_V2)
    }
}

impl Default for JsonMetadata {
    fn default() -> Self {
        Self::new(JsonSettings::default())
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
    use crate::vectors::json::variant::variant_field;

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

    #[test]
    fn test_json_metadata_layout_version_compatibility() -> serde_json::Result<()> {
        let legacy: JsonMetadata = serde_json::from_str(r#"{"json_settings":{}}"#)?;
        assert_eq!(legacy.layout_version, None);
        assert!(!legacy.is_version_2());

        let metadata = JsonMetadata::new(JsonSettings::default());
        assert!(metadata.is_version_2());
        let serialized = serde_json::to_string(&metadata)?;
        let deserialized: JsonMetadata = serde_json::from_str(&serialized)?;
        assert_eq!(deserialized.layout_version, Some(JSON2_LAYOUT_V2));
        assert!(deserialized.is_version_2());
        Ok(())
    }

    #[test]
    fn test_reject_non_json2_physical_layout() {
        let field = Field::new(
            "data",
            DataType::Struct(vec![Arc::new(Field::new("value", DataType::Binary, true))].into()),
            true,
        );

        assert!(matches!(
            Json2PhysicalLayout::try_from_root(&field),
            Err(crate::error::Error::InvalidJson2Layout { .. })
        ));
    }

    #[test]
    fn test_parse_json2_physical_layout() -> DatatypesResult<()> {
        let legacy = Field::new(
            "data",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    JSON2_REMAINDER_FIELD_NAME,
                    DataType::Binary,
                    true,
                ))]
                .into(),
            ),
            true,
        )
        .with_extension_type(Json2ExtensionType::new(Arc::new(JsonMetadata {
            json_settings: JsonSettings::default(),
            layout_version: None,
        })));
        let layout = Json2PhysicalLayout::try_from_root(&legacy)?;
        assert!(!layout.is_version_2());

        let v2 = Field::new(
            "data",
            DataType::Struct(
                vec![
                    Arc::new(variant_field(JSON2_REMAINDER_FIELD_NAME, true)),
                    Arc::new(Field::new("count", DataType::Int64, true)),
                ]
                .into(),
            ),
            true,
        )
        .with_extension_type(Json2ExtensionType::default());
        let layout = Json2PhysicalLayout::try_from_root(&v2)?;
        assert!(layout.is_version_2());
        assert_eq!(
            JSON2_REMAINDER_FIELD_NAME,
            json2_remainder_field(&v2)?.name()
        );
        Ok(())
    }

    #[test]
    fn test_validate_json2_v2_remainder() -> DatatypesResult<()> {
        let metadata = Json2ExtensionType::default();
        let missing = Field::new("data", DataType::Struct([].into()), true)
            .with_extension_type(metadata.clone());
        assert!(Json2PhysicalLayout::try_from_root(&missing)?.is_version_2());
        assert!(matches!(
            json2_remainder_field(&missing),
            Err(crate::error::Error::InvalidJson2Layout { .. })
        ));

        let invalid = Field::new(
            "data",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    JSON2_REMAINDER_FIELD_NAME,
                    DataType::Binary,
                    true,
                ))]
                .into(),
            ),
            true,
        )
        .with_extension_type(metadata);
        assert!(Json2PhysicalLayout::try_from_root(&invalid)?.is_version_2());
        assert!(matches!(
            json2_remainder_field(&invalid),
            Err(crate::error::Error::InvalidJson2Layout { .. })
        ));

        let future = Field::new("data", DataType::Struct([].into()), true).with_extension_type(
            Json2ExtensionType::new(Arc::new(JsonMetadata {
                json_settings: JsonSettings::default(),
                layout_version: Some(JSON2_LAYOUT_V2 + 1),
            })),
        );
        assert!(matches!(
            Json2PhysicalLayout::try_from_root(&future),
            Err(crate::error::Error::InvalidJson2Layout { .. })
        ));

        let legacy = Field::new(
            "data",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    JSON2_REMAINDER_FIELD_NAME,
                    DataType::Binary,
                    true,
                ))]
                .into(),
            ),
            true,
        )
        .with_extension_type(Json2ExtensionType::new(Arc::new(JsonMetadata::new_v1(
            JsonSettings::default(),
        ))));
        assert!(matches!(
            json2_remainder_field(&legacy),
            Err(crate::error::Error::InvalidJson2Layout { .. })
        ));
        Ok(())
    }
}
