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

//! Arrow extension type for native-histogram struct columns.

use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType, FieldRef};

/// Arrow extension type identifying a native-histogram struct column.
///
/// Applied to the struct field at parquet-write time so that readers can
/// identify native-histogram columns by extension (`greptime.histogram`) rather
/// than relying on the field name.
#[derive(Debug, Clone, Default)]
pub struct HistogramExtensionType;

impl ExtensionType for HistogramExtensionType {
    const NAME: &'static str = "greptime.histogram";
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
            DataType::Struct(_) => Ok(()),
            dt => Err(ArrowError::SchemaError(format!(
                "Unexpected data type {dt}"
            ))),
        }
    }

    fn try_new(data_type: &DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let ext = Self;
        ext.supports_data_type(data_type)?;
        Ok(ext)
    }
}

/// Check if this field is a native-histogram extension type.
pub fn is_histogram_extension_type(field: &FieldRef) -> bool {
    field.extension_type_name() == Some(HistogramExtensionType::NAME)
}

#[cfg(test)]
mod tests {
    use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
    use arrow_schema::{DataType, Field, Fields};

    use super::*;

    #[test]
    fn test_extension_name_and_detection() {
        assert_eq!(HistogramExtensionType::NAME, "greptime.histogram");

        // A plain struct field is not a histogram extension type.
        let empty: Fields = Vec::<Field>::new().into();
        let plain = std::sync::Arc::new(Field::new("s", DataType::Struct(empty), true));
        assert!(!is_histogram_extension_type(&plain));

        // Tagging the field with the extension makes it detectable.
        let mut tagged = (*plain).clone();
        tagged.metadata_mut().insert(
            EXTENSION_TYPE_NAME_KEY.to_string(),
            HistogramExtensionType::NAME.to_string(),
        );
        let tagged = std::sync::Arc::new(tagged);
        assert!(is_histogram_extension_type(&tagged));
    }

    #[test]
    fn test_supports_struct_only() {
        let ext = HistogramExtensionType;
        let empty: Fields = Vec::<Field>::new().into();
        assert!(ext.supports_data_type(&DataType::Struct(empty)).is_ok());
        assert!(ext.supports_data_type(&DataType::Int32).is_err());
    }
}
