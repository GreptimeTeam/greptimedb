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
use std::collections::BTreeSet;

use api::v1::alter_table_expr::Kind as AlterTableKind;
use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row};
use common_event_recorder::Event;
use common_event_recorder::error::{Result, SerializeEventSnafu};
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN, PHYSICAL_TABLE_ID_COLUMN, SCHEMA_NAME_COLUMN, TABLE_ID_COLUMN,
    TABLE_NAME_COLUMN, column_schemas, nullable_string, nullable_value,
};
use serde::Serialize;
use serde_json::Value as JsonValue;
use snafu::ResultExt;
use store_api::storage::TableId;

/// Current version of table DDL event payloads.
pub(crate) const TABLE_DDL_PAYLOAD_VERSION: u8 = 1;

/// A table DDL event type and its fixed domain schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableDdlEventType {
    CreateTable,
    CreateLogicalTables,
    AlterTable,
    AlterLogicalTables,
    DropTable,
    #[cfg(feature = "enterprise")]
    UndropTable,
    #[cfg(feature = "enterprise")]
    PurgeDroppedTable,
    TruncateTable,
}

impl TableDdlEventType {
    /// Returns the stable event type stored in the events table.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::CreateTable => "create_table",
            Self::CreateLogicalTables => "create_logical_tables",
            Self::AlterTable => "alter_table",
            Self::AlterLogicalTables => "alter_logical_tables",
            Self::DropTable => "drop_table",
            #[cfg(feature = "enterprise")]
            Self::UndropTable => "undrop_table",
            #[cfg(feature = "enterprise")]
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
pub(crate) struct TableDdlLocator {
    /// Catalog containing the table.
    pub(crate) catalog_name: Option<String>,
    /// Schema containing the table.
    pub(crate) schema_name: Option<String>,
    /// Table name.
    pub(crate) table_name: Option<String>,
    /// Table ID when known at this lifecycle point.
    pub(crate) table_id: Option<TableId>,
    /// Physical table ID for a logical table event.
    pub(crate) physical_table_id: Option<TableId>,
}

impl TableDdlLocator {
    /// Creates a locator from a fully qualified table name.
    pub(crate) fn new(
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
    #[cfg(feature = "enterprise")]
    pub(crate) fn from_table_id(table_id: TableId) -> Self {
        Self {
            table_id: Some(table_id),
            ..Default::default()
        }
    }

    /// Adds a table ID to the locator.
    pub(crate) fn with_table_id(mut self, table_id: TableId) -> Self {
        self.table_id = Some(table_id);
        self
    }

    /// Adds a physical table ID to a logical-table locator.
    pub(crate) fn with_physical_table_id(mut self, physical_table_id: TableId) -> Self {
        self.physical_table_id = Some(physical_table_id);
        self
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum TableDdlPayload {
    CreateTable(CreateTablePayload),
    CreateLogicalTables(CreateLogicalTablesPayload),
    AlterTable(AlterTablePayload),
    AlterLogicalTables(AlterLogicalTablesPayload),
    DropTable(DropTablePayload),
    #[cfg(feature = "enterprise")]
    UndropTable(UndropTablePayload),
    #[cfg(feature = "enterprise")]
    PurgeDroppedTable(PurgeDroppedTablePayload),
    TruncateTable(TruncateTablePayload),
}

#[derive(Debug, Serialize)]
struct CreateTablePayload {
    version: u8,
    create_if_not_exists: bool,
    engine: String,
}

#[derive(Debug, Serialize)]
struct CreateLogicalTablesPayload {
    version: u8,
    table_count: usize,
}

#[derive(Debug, Serialize)]
struct AlterTablePayload {
    version: u8,
    kind: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct AlterLogicalTablesPayload {
    version: u8,
    table_count: usize,
    kinds: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct DropTablePayload {
    version: u8,
    drop_if_exists: bool,
}

#[cfg(feature = "enterprise")]
#[derive(Debug, Serialize)]
struct UndropTablePayload {
    version: u8,
}

#[cfg(feature = "enterprise")]
#[derive(Debug, Serialize)]
struct PurgeDroppedTablePayload {
    version: u8,
}

#[derive(Debug, Serialize)]
struct TruncateTablePayload {
    version: u8,
    time_range_count: usize,
}

/// Returns the stable kind stored in an Alter Table payload, if supported.
pub(crate) fn alter_table_kind_name(kind: &AlterTableKind) -> Option<&'static str> {
    match kind {
        AlterTableKind::AddColumns(_) => Some("add_columns"),
        AlterTableKind::DropColumns(_) => Some("drop_columns"),
        AlterTableKind::RenameTable(_) => Some("rename_table"),
        AlterTableKind::ModifyColumnTypes(_) => Some("modify_column_types"),
        AlterTableKind::SetTableOptions(_) => Some("set_table_options"),
        AlterTableKind::UnsetTableOptions(_) => Some("unset_table_options"),
        AlterTableKind::SetIndex(_) => Some("set_index"),
        AlterTableKind::UnsetIndex(_) => Some("unset_index"),
        AlterTableKind::DropDefaults(_) => Some("drop_defaults"),
        AlterTableKind::SetIndexes(_) => Some("set_indexes"),
        AlterTableKind::UnsetIndexes(_) => Some("unset_indexes"),
        AlterTableKind::SetDefaults(_) => Some("set_defaults"),
        // Repartition is handled by RepartitionProcedure.
        AlterTableKind::Repartition(_) => None,
    }
}

/// Shared event representation used by table DDL procedures.
#[derive(Debug)]
pub(crate) struct TableDdlEvent {
    event_type: TableDdlEventType,
    locators: Vec<TableDdlLocator>,
    payload: Option<TableDdlPayload>,
}

impl TableDdlEvent {
    /// Builds the bounded event emitted when creating a table is submitted.
    pub(crate) fn create_table_submitted(
        locator: TableDdlLocator,
        create_if_not_exists: bool,
        engine: &str,
    ) -> Self {
        Self::submitted(
            TableDdlEventType::CreateTable,
            [locator],
            TableDdlPayload::CreateTable(CreateTablePayload {
                version: TABLE_DDL_PAYLOAD_VERSION,
                create_if_not_exists,
                engine: engine.to_string(),
            }),
        )
    }

    /// Builds the bounded event emitted when creating logical tables is submitted.
    pub(crate) fn create_logical_tables_submitted(
        locators: impl IntoIterator<Item = TableDdlLocator>,
        table_count: usize,
    ) -> Self {
        Self::submitted(
            TableDdlEventType::CreateLogicalTables,
            locators,
            TableDdlPayload::CreateLogicalTables(CreateLogicalTablesPayload {
                version: TABLE_DDL_PAYLOAD_VERSION,
                table_count,
            }),
        )
    }

    /// Builds the bounded event emitted when altering a table is submitted.
    pub(crate) fn alter_table_submitted(
        locator: TableDdlLocator,
        kind: Option<&'static str>,
    ) -> Self {
        Self::submitted(
            TableDdlEventType::AlterTable,
            [locator],
            TableDdlPayload::AlterTable(AlterTablePayload {
                version: TABLE_DDL_PAYLOAD_VERSION,
                kind,
            }),
        )
    }

    /// Builds the bounded event emitted when altering logical tables is submitted.
    pub(crate) fn alter_logical_tables_submitted(
        locators: impl IntoIterator<Item = TableDdlLocator>,
        table_count: usize,
        kinds: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        let kinds = kinds
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Self::submitted(
            TableDdlEventType::AlterLogicalTables,
            locators,
            TableDdlPayload::AlterLogicalTables(AlterLogicalTablesPayload {
                version: TABLE_DDL_PAYLOAD_VERSION,
                table_count,
                kinds,
            }),
        )
    }

    /// Builds the bounded event emitted when dropping a table is submitted.
    pub(crate) fn drop_table_submitted(locator: TableDdlLocator, drop_if_exists: bool) -> Self {
        Self::submitted(
            TableDdlEventType::DropTable,
            [locator],
            TableDdlPayload::DropTable(DropTablePayload {
                version: TABLE_DDL_PAYLOAD_VERSION,
                drop_if_exists,
            }),
        )
    }

    /// Builds the bounded event emitted when restoring a dropped table is submitted.
    #[cfg(feature = "enterprise")]
    pub(crate) fn undrop_table_submitted(locator: TableDdlLocator) -> Self {
        Self::submitted(
            TableDdlEventType::UndropTable,
            [locator],
            TableDdlPayload::UndropTable(UndropTablePayload {
                version: TABLE_DDL_PAYLOAD_VERSION,
            }),
        )
    }

    /// Builds the bounded event emitted when purging a dropped table is submitted.
    #[cfg(feature = "enterprise")]
    pub(crate) fn purge_dropped_table_submitted(locator: TableDdlLocator) -> Self {
        Self::submitted(
            TableDdlEventType::PurgeDroppedTable,
            [locator],
            TableDdlPayload::PurgeDroppedTable(PurgeDroppedTablePayload {
                version: TABLE_DDL_PAYLOAD_VERSION,
            }),
        )
    }

    /// Builds the bounded event emitted when truncating a table is submitted.
    pub(crate) fn truncate_table_submitted(
        locator: TableDdlLocator,
        time_range_count: usize,
    ) -> Self {
        Self::submitted(
            TableDdlEventType::TruncateTable,
            [locator],
            TableDdlPayload::TruncateTable(TruncateTablePayload {
                version: TABLE_DDL_PAYLOAD_VERSION,
                time_range_count,
            }),
        )
    }

    /// Builds a lifecycle event with stable object locators and no intent payload.
    pub(crate) fn lifecycle(
        event_type: TableDdlEventType,
        locators: impl IntoIterator<Item = TableDdlLocator>,
    ) -> Self {
        Self {
            event_type,
            locators: locators.into_iter().collect(),
            payload: None,
        }
    }

    /// Builds a Create Table success event containing the submitted locator and allocated ID.
    pub(crate) fn create_table_succeeded(locator: TableDdlLocator, table_id: TableId) -> Self {
        Self::lifecycle(
            TableDdlEventType::CreateTable,
            [locator.with_table_id(table_id)],
        )
    }

    /// Builds Create Logical Tables success rows from their allocated locators.
    pub(crate) fn create_logical_tables_succeeded(
        locators: impl IntoIterator<Item = TableDdlLocator>,
    ) -> Self {
        Self::lifecycle(TableDdlEventType::CreateLogicalTables, locators)
    }

    fn submitted(
        event_type: TableDdlEventType,
        locators: impl IntoIterator<Item = TableDdlLocator>,
        payload: TableDdlPayload,
    ) -> Self {
        Self {
            event_type,
            locators: locators.into_iter().collect(),
            payload: Some(payload),
        }
    }

    fn schema() -> Vec<ColumnSchema> {
        column_schemas([
            &CATALOG_NAME_COLUMN,
            &SCHEMA_NAME_COLUMN,
            &TABLE_NAME_COLUMN,
            &TABLE_ID_COLUMN,
        ])
    }

    fn locator_row(&self, locator: &TableDdlLocator) -> Row {
        let mut values = vec![
            nullable_string(locator.catalog_name.as_deref()),
            nullable_string(locator.schema_name.as_deref()),
            nullable_string(locator.table_name.as_deref()),
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
        match &self.payload {
            Some(payload) => serde_json::to_value(payload).context(SerializeEventSnafu),
            None => Ok(JsonValue::Null),
        }
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        let mut schema = Self::schema();
        if self.event_type.has_physical_table_id() {
            schema.push(PHYSICAL_TABLE_ID_COLUMN.column_schema());
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

fn nullable_table_id(value: Option<TableId>) -> api::v1::Value {
    nullable_value(value.map(ValueData::U32Value))
}
