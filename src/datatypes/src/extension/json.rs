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

use std::sync::Arc;

use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType, FieldRef};
use serde::{Deserialize, Serialize};

use crate::json::JsonStructureSettings;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JsonMetadata {
    /// Indicates how to handle JSON is stored in underlying data type
    ///
    /// This field can be `None` for data is converted to complete structured in-memory form.
    pub json_structure_settings: Option<JsonStructureSettings>,
}

#[derive(Debug, Clone)]
pub struct JsonExtensionType(Arc<JsonMetadata>);

impl JsonExtensionType {
    pub fn new(metadata: Arc<JsonMetadata>) -> Self {
        JsonExtensionType(metadata)
    }
}

impl ExtensionType for JsonExtensionType {
    const NAME: &'static str = "greptime.json";
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

/// Check if this field is to be treated as json extension type.
pub fn is_json_extension_type(field: &FieldRef) -> bool {
    field.extension_type_name() == Some(JsonExtensionType::NAME)
}
