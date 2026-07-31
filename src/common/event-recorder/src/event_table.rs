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
/// The canonical Flow name dimension.
pub const FLOW_NAME_COLUMN: EventTableColumn =
    EventTableColumn::new("flow_name", ColumnDataType::String, SemanticType::Field);
/// The canonical Flow identifier dimension.
pub const FLOW_ID_COLUMN: EventTableColumn =
    EventTableColumn::new("flow_id", ColumnDataType::Uint32, SemanticType::Field);
/// The canonical View name dimension.
pub const VIEW_NAME_COLUMN: EventTableColumn =
    EventTableColumn::new("view_name", ColumnDataType::String, SemanticType::Field);
/// The canonical View identifier dimension.
pub const VIEW_ID_COLUMN: EventTableColumn =
    EventTableColumn::new("view_id", ColumnDataType::Uint32, SemanticType::Field);
/// The canonical Kafka topic name dimension.
pub const TOPIC_NAME_COLUMN: EventTableColumn =
    EventTableColumn::new("topic_name", ColumnDataType::String, SemanticType::Field);
/// The requested WAL prune boundary. It is only an attempted boundary on non-`Succeeded`
/// procedure events.
pub const PRUNABLE_ENTRY_ID_COLUMN: EventTableColumn = EventTableColumn::new(
    "prunable_entry_id",
    ColumnDataType::Uint64,
    SemanticType::Field,
);
/// The canonical Kafka latest offset, which is an exclusive upper bound.
pub const LATEST_OFFSET_COLUMN: EventTableColumn =
    EventTableColumn::new("latest_offset", ColumnDataType::Uint64, SemanticType::Field);
/// The canonical per-region GC report field.
pub const GC_REPORT_COLUMN: EventTableColumn =
    EventTableColumn::json_binary("gc_report", ColumnDataType::Binary, SemanticType::Field);

/// The canonical table name field for table DDL events.
pub const TABLE_NAME_COLUMN: EventTableColumn =
    EventTableColumn::new("table_name", ColumnDataType::String, SemanticType::Field);
/// The canonical table identifier field for table DDL events.
pub const TABLE_ID_COLUMN: EventTableColumn =
    EventTableColumn::new("table_id", ColumnDataType::Uint32, SemanticType::Field);
/// The canonical physical table identifier dimension.
pub const PHYSICAL_TABLE_ID_COLUMN: EventTableColumn = EventTableColumn::new(
    "physical_table_id",
    ColumnDataType::Uint32,
    SemanticType::Field,
);
/// The canonical region identifier field for region events.
pub const REGION_ID_COLUMN: EventTableColumn =
    EventTableColumn::new("region_id", ColumnDataType::Uint64, SemanticType::Field);
/// The canonical region number field for region events.
pub const REGION_NUMBER_COLUMN: EventTableColumn =
    EventTableColumn::new("region_number", ColumnDataType::Uint32, SemanticType::Field);
/// The canonical region migration trigger reason field.
pub const REGION_MIGRATION_TRIGGER_REASON_COLUMN: EventTableColumn = EventTableColumn::new(
    "region_migration_trigger_reason",
    ColumnDataType::String,
    SemanticType::Field,
);
/// The canonical region migration source node identifier field.
pub const REGION_MIGRATION_SRC_NODE_ID_COLUMN: EventTableColumn = EventTableColumn::new(
    "region_migration_src_node_id",
    ColumnDataType::Uint64,
    SemanticType::Field,
);
/// The canonical region migration source peer address field.
pub const REGION_MIGRATION_SRC_PEER_ADDR_COLUMN: EventTableColumn = EventTableColumn::new(
    "region_migration_src_peer_addr",
    ColumnDataType::String,
    SemanticType::Field,
);
/// The canonical region migration destination node identifier field.
pub const REGION_MIGRATION_DST_NODE_ID_COLUMN: EventTableColumn = EventTableColumn::new(
    "region_migration_dst_node_id",
    ColumnDataType::Uint64,
    SemanticType::Field,
);
/// The canonical region migration destination peer address field.
pub const REGION_MIGRATION_DST_PEER_ADDR_COLUMN: EventTableColumn = EventTableColumn::new(
    "region_migration_dst_peer_addr",
    ColumnDataType::String,
    SemanticType::Field,
);
/// The canonical parent procedure identifier field for child procedure events.
pub const PARENT_PROCEDURE_ID_COLUMN: EventTableColumn = EventTableColumn::new(
    "parent_procedure_id",
    ColumnDataType::String,
    SemanticType::Field,
);
/// The canonical repartition group identifier field.
pub const REPARTITION_GROUP_ID_COLUMN: EventTableColumn = EventTableColumn::new(
    "repartition_group_id",
    ColumnDataType::String,
    SemanticType::Field,
);
/// The canonical repartition source region identifier field.
pub const SOURCE_REGION_ID_COLUMN: EventTableColumn = EventTableColumn::new(
    "source_region_id",
    ColumnDataType::Uint64,
    SemanticType::Field,
);
/// The canonical repartition source region number field.
pub const SOURCE_REGION_NUMBER_COLUMN: EventTableColumn = EventTableColumn::new(
    "source_region_number",
    ColumnDataType::Uint32,
    SemanticType::Field,
);
/// The canonical repartition source partition expression field.
pub const SOURCE_PARTITION_EXPR_COLUMN: EventTableColumn = EventTableColumn::new(
    "source_partition_expr",
    ColumnDataType::String,
    SemanticType::Field,
);
/// The canonical repartition target region identifier field.
pub const TARGET_REGION_ID_COLUMN: EventTableColumn = EventTableColumn::new(
    "target_region_id",
    ColumnDataType::Uint64,
    SemanticType::Field,
);
/// The canonical repartition target region number field.
pub const TARGET_REGION_NUMBER_COLUMN: EventTableColumn = EventTableColumn::new(
    "target_region_number",
    ColumnDataType::Uint32,
    SemanticType::Field,
);
/// The canonical repartition target partition expression field.
pub const TARGET_PARTITION_EXPR_COLUMN: EventTableColumn = EventTableColumn::new(
    "target_partition_expr",
    ColumnDataType::String,
    SemanticType::Field,
);

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

/// Builds a nullable API JSONB value.
pub fn nullable_json(value: Option<&serde_json::Value>) -> Value {
    nullable_value(value.map(|value| ValueData::BinaryValue(jsonb::Value::from(value).to_vec())))
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
    fn flow_dimension_schema_preserves_names_types_semantics_and_order() {
        assert_eq!(
            column_schemas([&FLOW_NAME_COLUMN, &FLOW_ID_COLUMN]),
            [
                ("flow_name", ColumnDataType::String),
                ("flow_id", ColumnDataType::Uint32),
            ]
            .map(|(column_name, datatype)| ColumnSchema {
                column_name: column_name.to_string(),
                datatype: datatype.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            })
        );
    }

    #[test]
    fn table_dimension_schema_preserves_names_types_semantics_and_order() {
        assert_eq!(
            column_schemas([
                &TABLE_NAME_COLUMN,
                &TABLE_ID_COLUMN,
                &PHYSICAL_TABLE_ID_COLUMN,
            ]),
            [
                ("table_name", ColumnDataType::String),
                ("table_id", ColumnDataType::Uint32),
                ("physical_table_id", ColumnDataType::Uint32),
            ]
            .map(|(column_name, datatype)| ColumnSchema {
                column_name: column_name.to_string(),
                datatype: datatype.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            })
        );
    }

    #[test]
    fn view_dimension_schema_preserves_names_types_semantics_and_order() {
        assert_eq!(
            column_schemas([&VIEW_NAME_COLUMN, &VIEW_ID_COLUMN]),
            [
                ("view_name", ColumnDataType::String),
                ("view_id", ColumnDataType::Uint32),
            ]
            .map(|(column_name, datatype)| ColumnSchema {
                column_name: column_name.to_string(),
                datatype: datatype.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            })
        );
    }

    #[test]
    fn region_dimension_schema_preserves_names_types_semantics_and_order() {
        assert_field_columns(
            [
                &REGION_ID_COLUMN,
                &REGION_NUMBER_COLUMN,
                &REGION_MIGRATION_TRIGGER_REASON_COLUMN,
                &REGION_MIGRATION_SRC_NODE_ID_COLUMN,
                &REGION_MIGRATION_SRC_PEER_ADDR_COLUMN,
                &REGION_MIGRATION_DST_NODE_ID_COLUMN,
                &REGION_MIGRATION_DST_PEER_ADDR_COLUMN,
            ],
            [
                ("region_id", ColumnDataType::Uint64),
                ("region_number", ColumnDataType::Uint32),
                ("region_migration_trigger_reason", ColumnDataType::String),
                ("region_migration_src_node_id", ColumnDataType::Uint64),
                ("region_migration_src_peer_addr", ColumnDataType::String),
                ("region_migration_dst_node_id", ColumnDataType::Uint64),
                ("region_migration_dst_peer_addr", ColumnDataType::String),
            ],
        );
    }

    #[test]
    fn repartition_dimension_schema_preserves_names_types_semantics_and_order() {
        assert_field_columns(
            [
                &TABLE_NAME_COLUMN,
                &TABLE_ID_COLUMN,
                &PARENT_PROCEDURE_ID_COLUMN,
                &REPARTITION_GROUP_ID_COLUMN,
                &SOURCE_REGION_ID_COLUMN,
                &SOURCE_REGION_NUMBER_COLUMN,
                &SOURCE_PARTITION_EXPR_COLUMN,
                &TARGET_REGION_ID_COLUMN,
                &TARGET_REGION_NUMBER_COLUMN,
                &TARGET_PARTITION_EXPR_COLUMN,
            ],
            [
                ("table_name", ColumnDataType::String),
                ("table_id", ColumnDataType::Uint32),
                ("parent_procedure_id", ColumnDataType::String),
                ("repartition_group_id", ColumnDataType::String),
                ("source_region_id", ColumnDataType::Uint64),
                ("source_region_number", ColumnDataType::Uint32),
                ("source_partition_expr", ColumnDataType::String),
                ("target_region_id", ColumnDataType::Uint64),
                ("target_region_number", ColumnDataType::Uint32),
                ("target_partition_expr", ColumnDataType::String),
            ],
        );
    }

    #[test]
    fn wal_prune_schema_preserves_names_types_semantics_and_order() {
        assert_field_columns(
            [
                &TOPIC_NAME_COLUMN,
                &PRUNABLE_ENTRY_ID_COLUMN,
                &LATEST_OFFSET_COLUMN,
            ],
            [
                ("topic_name", ColumnDataType::String),
                ("prunable_entry_id", ColumnDataType::Uint64),
                ("latest_offset", ColumnDataType::Uint64),
            ],
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

    fn assert_field_columns<const N: usize>(
        columns: [&EventTableColumn; N],
        expected: [(&str, ColumnDataType); N],
    ) {
        assert_eq!(
            column_schemas(columns),
            expected.map(|(column_name, datatype)| ColumnSchema {
                column_name: column_name.to_string(),
                datatype: datatype.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            })
        );
    }
}
