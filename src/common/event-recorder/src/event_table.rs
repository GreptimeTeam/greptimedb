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

use api::v1::column_data_type_extension::TypeExt;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnSchema, JsonTypeExtension, SemanticType, Value,
};

/// A canonical column in the shared event table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventTableColumn {
    name: &'static str,
    datatype: ColumnDataType,
    semantic_type: SemanticType,
    json_binary: bool,
}

impl EventTableColumn {
    const fn new(
        name: &'static str,
        datatype: ColumnDataType,
        semantic_type: SemanticType,
    ) -> Self {
        Self {
            name,
            datatype,
            semantic_type,
            json_binary: false,
        }
    }

    const fn json_binary(
        name: &'static str,
        datatype: ColumnDataType,
        semantic_type: SemanticType,
    ) -> Self {
        Self {
            name,
            datatype,
            semantic_type,
            json_binary: true,
        }
    }

    /// Returns the canonical column name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Builds the canonical API schema for this column.
    pub fn column_schema(&self) -> ColumnSchema {
        ColumnSchema {
            column_name: self.name.to_string(),
            datatype: self.datatype.into(),
            semantic_type: self.semantic_type.into(),
            datatype_extension: self.json_binary.then(|| ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            }),
            ..Default::default()
        }
    }
}

/// The canonical event type column.
pub const TYPE_COLUMN: EventTableColumn =
    EventTableColumn::new("type", ColumnDataType::String, SemanticType::Tag);
/// The canonical event payload column.
pub const PAYLOAD_COLUMN: EventTableColumn =
    EventTableColumn::json_binary("payload", ColumnDataType::Binary, SemanticType::Field);
/// The canonical event timestamp column.
pub const TIMESTAMP_COLUMN: EventTableColumn = EventTableColumn::new(
    "timestamp",
    ColumnDataType::TimestampNanosecond,
    SemanticType::Timestamp,
);
/// The canonical procedure identifier envelope column.
pub const PROCEDURE_ID_COLUMN: EventTableColumn =
    EventTableColumn::new("procedure_id", ColumnDataType::String, SemanticType::Field);
/// The canonical procedure state envelope column.
pub const PROCEDURE_STATE_COLUMN: EventTableColumn = EventTableColumn::new(
    "procedure_state",
    ColumnDataType::String,
    SemanticType::Field,
);
/// The canonical procedure error envelope column.
pub const PROCEDURE_ERROR_COLUMN: EventTableColumn = EventTableColumn::new(
    "procedure_error",
    ColumnDataType::String,
    SemanticType::Field,
);
/// The canonical procedure trigger envelope column.
pub const PROCEDURE_TRIGGER_COLUMN: EventTableColumn = EventTableColumn::new(
    "procedure_trigger",
    ColumnDataType::String,
    SemanticType::Field,
);
/// The canonical catalog name dimension.
pub const CATALOG_NAME_COLUMN: EventTableColumn =
    EventTableColumn::new("catalog_name", ColumnDataType::String, SemanticType::Field);
/// The canonical schema name dimension.
pub const SCHEMA_NAME_COLUMN: EventTableColumn =
    EventTableColumn::new("schema_name", ColumnDataType::String, SemanticType::Field);

/// Builds API schemas from canonical event-table columns while preserving their order.
pub fn column_schemas<'a>(
    columns: impl IntoIterator<Item = &'a EventTableColumn>,
) -> Vec<ColumnSchema> {
    columns
        .into_iter()
        .map(EventTableColumn::column_schema)
        .collect()
}

/// Builds the canonical base schema for every recorded event.
pub fn base_column_schemas() -> Vec<ColumnSchema> {
    column_schemas([&TYPE_COLUMN, &PAYLOAD_COLUMN, &TIMESTAMP_COLUMN])
}

/// Builds the canonical procedure event envelope schema.
pub fn procedure_event_column_schemas() -> Vec<ColumnSchema> {
    column_schemas([
        &PROCEDURE_ID_COLUMN,
        &PROCEDURE_STATE_COLUMN,
        &PROCEDURE_ERROR_COLUMN,
        &PROCEDURE_TRIGGER_COLUMN,
    ])
}

/// Builds an API value from an optional typed value.
pub fn nullable_value(value: Option<ValueData>) -> Value {
    Value { value_data: value }
}

/// Builds a nullable API string value.
pub fn nullable_string<T>(value: Option<T>) -> Value
where
    T: AsRef<str>,
{
    nullable_value(value.map(|value| ValueData::StringValue(value.as_ref().to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_schema_preserves_names_types_semantics_extensions_and_order() {
        assert_eq!(
            base_column_schemas(),
            vec![
                ColumnSchema {
                    column_name: "type".to_string(),
                    datatype: ColumnDataType::String.into(),
                    semantic_type: SemanticType::Tag.into(),
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "payload".to_string(),
                    datatype: ColumnDataType::Binary.into(),
                    semantic_type: SemanticType::Field.into(),
                    datatype_extension: Some(ColumnDataTypeExtension {
                        type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
                    }),
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "timestamp".to_string(),
                    datatype: ColumnDataType::TimestampNanosecond.into(),
                    semantic_type: SemanticType::Timestamp.into(),
                    ..Default::default()
                },
            ]
        );
    }

    #[test]
    fn procedure_envelope_schema_preserves_names_types_semantics_and_order() {
        assert_eq!(
            procedure_event_column_schemas(),
            [
                "procedure_id",
                "procedure_state",
                "procedure_error",
                "procedure_trigger",
            ]
            .map(|column_name| ColumnSchema {
                column_name: column_name.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            })
        );
    }

    #[test]
    fn shared_dimension_schema_preserves_names_types_semantics_and_order() {
        assert_eq!(
            column_schemas([&CATALOG_NAME_COLUMN, &SCHEMA_NAME_COLUMN]),
            ["catalog_name", "schema_name"].map(|column_name| ColumnSchema {
                column_name: column_name.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            })
        );
    }

    #[test]
    fn nullable_values_preserve_types_and_nulls() {
        assert_eq!(
            nullable_string(Some("catalog")),
            Value {
                value_data: Some(ValueData::StringValue("catalog".to_string()))
            }
        );
        assert_eq!(nullable_value(None), Value { value_data: None });
        assert_eq!(
            nullable_value(Some(ValueData::BoolValue(true))),
            Value {
                value_data: Some(ValueData::BoolValue(true))
            }
        );
    }
}
