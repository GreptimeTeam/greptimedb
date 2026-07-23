// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, SemanticType, Value};
use common_event_recorder::Event;
use common_event_recorder::error::{Result, SerializeEventSnafu};
use serde::Serialize;
use serde_json::Value as JsonValue;
use snafu::ResultExt;
use store_api::storage::TableId;

/// Current version of table DDL event payloads.
pub const TABLE_DDL_PAYLOAD_VERSION: u32 = 1;

pub const EVENTS_TABLE_CATALOG_NAME_COLUMN_NAME: &str = "catalog_name";
pub const EVENTS_TABLE_SCHEMA_NAME_COLUMN_NAME: &str = "schema_name";
pub const EVENTS_TABLE_TABLE_NAME_COLUMN_NAME: &str = "table_name";
pub const EVENTS_TABLE_TABLE_ID_COLUMN_NAME: &str = "table_id";
pub const EVENTS_TABLE_PHYSICAL_TABLE_ID_COLUMN_NAME: &str = "physical_table_id";

/// A table DDL event type and its fixed domain schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableDdlEventType {
    CreateTable,
    CreateLogicalTables,
    AlterTable,
    AlterLogicalTables,
    DropTable,
    UndropTable,
    PurgeDroppedTable,
    TruncateTable,
}

impl TableDdlEventType {
    /// Returns the stable event type stored in the events table.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CreateTable => "create_table",
            Self::CreateLogicalTables => "create_logical_tables",
            Self::AlterTable => "alter_table",
            Self::AlterLogicalTables => "alter_logical_tables",
            Self::DropTable => "drop_table",
            Self::UndropTable => "undrop_table",
            Self::PurgeDroppedTable => "purge_dropped_table",
            Self::TruncateTable => "truncate_table",
        }
    }

    const fn has_physical_table_id(self) -> bool {
        matches!(self, Self::CreateLogicalTables | Self::AlterLogicalTables)
    }
}

/// Nullable table locator columns stored alongside a table DDL event.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableDdlLocator {
    /// Catalog containing the table.
    pub catalog_name: Option<String>,
    /// Schema containing the table.
    pub schema_name: Option<String>,
    /// Table name.
    pub table_name: Option<String>,
    /// Table ID when known at this lifecycle point.
    pub table_id: Option<TableId>,
    /// Physical table ID for a logical table event.
    pub physical_table_id: Option<TableId>,
}

impl TableDdlLocator {
    /// Creates a locator from a fully qualified table name.
    pub fn new(
        catalog_name: impl Into<String>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            catalog_name: Some(catalog_name.into()),
            schema_name: Some(schema_name.into()),
            table_name: Some(table_name.into()),
            ..Default::default()
        }
    }

    /// Creates a locator containing only a table ID.
    pub fn from_table_id(table_id: TableId) -> Self {
        Self {
            table_id: Some(table_id),
            ..Default::default()
        }
    }

    /// Adds a table ID to the locator.
    pub fn with_table_id(mut self, table_id: TableId) -> Self {
        self.table_id = Some(table_id);
        self
    }

    /// Adds a physical table ID to a logical-table locator.
    pub fn with_physical_table_id(mut self, physical_table_id: TableId) -> Self {
        self.physical_table_id = Some(physical_table_id);
        self
    }
}

/// Wraps rich submitted data in the versioned table DDL payload envelope.
pub fn versioned_table_ddl_payload<T: Serialize>(data: T) -> Result<JsonValue> {
    #[derive(Serialize)]
    struct VersionedPayload<T> {
        version: u32,
        data: T,
    }

    serde_json::to_value(VersionedPayload {
        version: TABLE_DDL_PAYLOAD_VERSION,
        data,
    })
    .context(SerializeEventSnafu)
}

/// Builds a versioned payload, replacing serialization failures with a fixed summary.
///
/// The fallback deliberately excludes the serializer error and input data so a
/// submitted event can retain its locator rows without exposing internal details.
pub fn versioned_table_ddl_payload_or_error<T: Serialize>(data: T) -> JsonValue {
    versioned_table_ddl_payload(data).unwrap_or_else(|_| {
        serde_json::json!({
            "version": TABLE_DDL_PAYLOAD_VERSION,
            "error": {
                "code": "payload_serialization_failed"
            }
        })
    })
}

/// Shared event representation used by table DDL procedures.
#[derive(Debug)]
pub struct TableDdlEvent {
    event_type: TableDdlEventType,
    locators: Vec<TableDdlLocator>,
    payload: JsonValue,
}

impl TableDdlEvent {
    /// Builds a rich submitted event for one table row.
    pub fn submitted(
        event_type: TableDdlEventType,
        locator: TableDdlLocator,
        payload: JsonValue,
    ) -> Self {
        Self::submitted_for_tables(event_type, [locator], payload)
    }

    /// Builds a rich submitted event with one row per logical table locator.
    pub fn submitted_for_tables(
        event_type: TableDdlEventType,
        locators: impl IntoIterator<Item = TableDdlLocator>,
        payload: JsonValue,
    ) -> Self {
        Self {
            event_type,
            locators: locators.into_iter().collect(),
            payload,
        }
    }

    /// Builds a lightweight lifecycle event with null domain columns and payload.
    pub fn lifecycle(event_type: TableDdlEventType) -> Self {
        Self {
            event_type,
            locators: vec![TableDdlLocator::default()],
            payload: JsonValue::Null,
        }
    }

    /// Builds a Create Table success event containing only the allocated table ID.
    pub fn create_table_succeeded(table_id: TableId) -> Self {
        Self {
            event_type: TableDdlEventType::CreateTable,
            locators: vec![TableDdlLocator::from_table_id(table_id)],
            payload: JsonValue::Null,
        }
    }

    /// Builds Create Logical Tables success rows containing only allocated table IDs.
    pub fn create_logical_tables_succeeded(table_ids: impl IntoIterator<Item = TableId>) -> Self {
        Self {
            event_type: TableDdlEventType::CreateLogicalTables,
            locators: table_ids
                .into_iter()
                .map(TableDdlLocator::from_table_id)
                .collect(),
            payload: JsonValue::Null,
        }
    }

    fn schema() -> Vec<ColumnSchema> {
        [
            (
                EVENTS_TABLE_CATALOG_NAME_COLUMN_NAME,
                ColumnDataType::String,
            ),
            (EVENTS_TABLE_SCHEMA_NAME_COLUMN_NAME, ColumnDataType::String),
            (EVENTS_TABLE_TABLE_NAME_COLUMN_NAME, ColumnDataType::String),
            (EVENTS_TABLE_TABLE_ID_COLUMN_NAME, ColumnDataType::Uint32),
        ]
        .into_iter()
        .map(|(column_name, datatype)| ColumnSchema {
            column_name: column_name.to_string(),
            datatype: datatype.into(),
            semantic_type: SemanticType::Field.into(),
            ..Default::default()
        })
        .collect()
    }

    fn locator_row(&self, locator: &TableDdlLocator) -> Row {
        let mut values = vec![
            nullable_string(&locator.catalog_name),
            nullable_string(&locator.schema_name),
            nullable_string(&locator.table_name),
            nullable_table_id(locator.table_id),
        ];
        if self.event_type.has_physical_table_id() {
            values.push(nullable_table_id(locator.physical_table_id));
        }
        Row { values }
    }
}

impl Event for TableDdlEvent {
    fn event_type(&self) -> &str {
        self.event_type.as_str()
    }

    fn json_payload(&self) -> Result<JsonValue> {
        Ok(self.payload.clone())
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        let mut schema = Self::schema();
        if self.event_type.has_physical_table_id() {
            schema.push(ColumnSchema {
                column_name: EVENTS_TABLE_PHYSICAL_TABLE_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint32.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            });
        }
        schema
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        Ok(self
            .locators
            .iter()
            .map(|locator| self.locator_row(locator))
            .collect())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn nullable_string(value: &Option<String>) -> Value {
    value
        .as_ref()
        .map(|value| ValueData::StringValue(value.clone()).into())
        .unwrap_or_default()
}

fn nullable_table_id(value: Option<TableId>) -> Value {
    value
        .map(|value| ValueData::U32Value(value).into())
        .unwrap_or_default()
}
