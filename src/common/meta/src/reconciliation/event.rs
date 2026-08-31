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

use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row};
use common_event_recorder::Event;
use common_event_recorder::error::{Result, SerializeEventSnafu};
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN, PHYSICAL_TABLE_ID_COLUMN, SCHEMA_NAME_COLUMN, TABLE_ID_COLUMN,
    TABLE_NAME_COLUMN, column_schemas, nullable_string, nullable_value,
};
use serde::Serialize;
use snafu::ResultExt;
use store_api::storage::TableId;

use crate::reconciliation::ResolveStrategy;

/// Stable event type stored for logical table reconciliation procedures.
pub(crate) const RECONCILE_LOGICAL_TABLES_EVENT_TYPE: &str = "reconcile_logical_tables";
/// Stable event type stored for physical table reconciliation procedures.
pub(crate) const RECONCILE_TABLE_EVENT_TYPE: &str = "reconcile_table";
const PAYLOAD_VERSION: u8 = 1;

/// Nullable object locators shared by all reconciliation event types.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReconciliationLocator {
    catalog_name: Option<String>,
    schema_name: Option<String>,
    table_name: Option<String>,
    table_id: Option<TableId>,
    physical_table_id: Option<TableId>,
}

impl ReconciliationLocator {
    /// Creates a locator for a physical table with its fully qualified name and ID.
    pub(crate) fn physical_table(
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_id: TableId,
    ) -> Self {
        Self {
            catalog_name: Some(catalog_name.to_string()),
            schema_name: Some(schema_name.to_string()),
            table_name: Some(table_name.to_string()),
            table_id: Some(table_id),
            ..Default::default()
        }
    }

    /// Creates a locator that links a logical table to its physical table.
    pub(crate) fn logical_table(
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_id: TableId,
        physical_table_id: TableId,
    ) -> Self {
        Self {
            catalog_name: Some(catalog_name.to_string()),
            schema_name: Some(schema_name.to_string()),
            table_name: Some(table_name.to_string()),
            table_id: Some(table_id),
            physical_table_id: Some(physical_table_id),
        }
    }

    fn schema() -> Vec<ColumnSchema> {
        column_schemas([
            &CATALOG_NAME_COLUMN,
            &SCHEMA_NAME_COLUMN,
            &TABLE_NAME_COLUMN,
            &TABLE_ID_COLUMN,
            &PHYSICAL_TABLE_ID_COLUMN,
        ])
    }

    fn row(&self) -> Row {
        Row {
            values: vec![
                nullable_string(self.catalog_name.as_deref()),
                nullable_string(self.schema_name.as_deref()),
                nullable_string(self.table_name.as_deref()),
                nullable_table_id(self.table_id),
                nullable_table_id(self.physical_table_id),
            ],
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ReconcileTablePayload {
    Submitted(TableSubmittedPayload),
    Result(TableResultPayload),
}

#[derive(Debug, Serialize)]
struct TableSubmittedPayload {
    version: u8,
    resolve_strategy: &'static str,
    is_subprocedure: bool,
}

#[derive(Debug, Serialize)]
struct TableResultPayload {
    version: u8,
    complete: bool,
    metadata_state: Option<&'static str>,
    resolution_strategy_applied: Option<&'static str>,
    resolved_column_count: Option<usize>,
    scanned_region_count: usize,
    updated_region_count: usize,
    table_info_updated: bool,
    last_completed_phase: Option<&'static str>,
}

/// Event representation for physical table reconciliation.
#[derive(Debug)]
pub(crate) struct ReconcileTableEvent {
    locator: ReconciliationLocator,
    payload: Option<ReconcileTablePayload>,
}

impl ReconcileTableEvent {
    /// Builds the bounded intent event emitted when table reconciliation is submitted.
    pub(crate) fn table_submitted(
        locator: ReconciliationLocator,
        resolve_strategy: ResolveStrategy,
        is_subprocedure: bool,
    ) -> Self {
        Self {
            locator,
            payload: Some(ReconcileTablePayload::Submitted(TableSubmittedPayload {
                version: PAYLOAD_VERSION,
                resolve_strategy: resolve_strategy_name(resolve_strategy),
                is_subprocedure,
            })),
        }
    }

    /// Builds a terminal event from the bounded reconciliation result summary.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn table_result(
        locator: ReconciliationLocator,
        complete: bool,
        metadata_state: Option<&'static str>,
        resolution_strategy_applied: Option<ResolveStrategy>,
        resolved_column_count: Option<usize>,
        scanned_region_count: usize,
        updated_region_count: usize,
        table_info_updated: bool,
        last_completed_phase: Option<&'static str>,
    ) -> Self {
        Self {
            locator,
            payload: Some(ReconcileTablePayload::Result(TableResultPayload {
                version: PAYLOAD_VERSION,
                complete,
                metadata_state,
                resolution_strategy_applied: resolution_strategy_applied.map(resolve_strategy_name),
                resolved_column_count,
                scanned_region_count,
                updated_region_count,
                table_info_updated,
                last_completed_phase,
            })),
        }
    }

    /// Builds a table lifecycle event whose reconciliation payload is null.
    pub(crate) fn table_lifecycle(locator: ReconciliationLocator) -> Self {
        Self {
            locator,
            payload: None,
        }
    }
}

impl Event for ReconcileTableEvent {
    fn event_type(&self) -> &str {
        RECONCILE_TABLE_EVENT_TYPE
    }

    fn json_payload(&self) -> Result<serde_json::Value> {
        match &self.payload {
            Some(payload) => serde_json::to_value(payload).context(SerializeEventSnafu),
            None => Ok(serde_json::Value::Null),
        }
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        ReconciliationLocator::schema()
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        Ok(vec![self.locator.row()])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ReconcileLogicalTablesPayload {
    Submitted(LogicalTablesSubmittedPayload),
    Result(LogicalTablesResultPayload),
}

#[derive(Debug, Serialize)]
struct LogicalTablesSubmittedPayload {
    version: u8,
    logical_table_count: usize,
    is_subprocedure: bool,
}

#[derive(Debug, Serialize)]
struct LogicalTablesResultPayload {
    version: u8,
    complete: bool,
    processed_table_count: usize,
    metadata_consistent_table_count: usize,
    metadata_inconsistent_table_count: usize,
    missing_region_table_count: usize,
    resolved_column_count: usize,
    scanned_region_count: usize,
    created_region_table_count: usize,
    created_region_count: usize,
    updated_table_info_count: usize,
    last_completed_phase: Option<&'static str>,
}

/// Event representation for logical table reconciliation.
#[derive(Debug)]
pub(crate) struct ReconcileLogicalTablesEvent {
    locators: Vec<ReconciliationLocator>,
    payload: Option<ReconcileLogicalTablesPayload>,
}

impl ReconcileLogicalTablesEvent {
    /// Builds the bounded intent event emitted when logical table reconciliation is submitted.
    pub(crate) fn submitted(locators: Vec<ReconciliationLocator>, is_subprocedure: bool) -> Self {
        let logical_table_count = locators.len();
        Self {
            locators,
            payload: Some(ReconcileLogicalTablesPayload::Submitted(
                LogicalTablesSubmittedPayload {
                    version: PAYLOAD_VERSION,
                    logical_table_count,
                    is_subprocedure,
                },
            )),
        }
    }

    /// Builds a terminal event from the bounded logical table result summary.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn result(
        locators: Vec<ReconciliationLocator>,
        complete: bool,
        metadata_consistent_table_count: usize,
        metadata_inconsistent_table_count: usize,
        missing_region_table_count: usize,
        resolved_column_count: usize,
        scanned_region_count: usize,
        created_region_table_count: usize,
        created_region_count: usize,
        updated_table_info_count: usize,
        last_completed_phase: Option<&'static str>,
    ) -> Self {
        Self {
            locators,
            payload: Some(ReconcileLogicalTablesPayload::Result(
                LogicalTablesResultPayload {
                    version: PAYLOAD_VERSION,
                    complete,
                    processed_table_count: metadata_consistent_table_count
                        + metadata_inconsistent_table_count
                        + missing_region_table_count,
                    metadata_consistent_table_count,
                    metadata_inconsistent_table_count,
                    missing_region_table_count,
                    resolved_column_count,
                    scanned_region_count,
                    created_region_table_count,
                    created_region_count,
                    updated_table_info_count,
                    last_completed_phase,
                },
            )),
        }
    }

    /// Builds a lifecycle event whose reconciliation payload is null.
    pub(crate) fn lifecycle(locators: Vec<ReconciliationLocator>) -> Self {
        Self {
            locators,
            payload: None,
        }
    }
}

impl Event for ReconcileLogicalTablesEvent {
    fn event_type(&self) -> &str {
        RECONCILE_LOGICAL_TABLES_EVENT_TYPE
    }

    fn json_payload(&self) -> Result<serde_json::Value> {
        match &self.payload {
            Some(payload) => serde_json::to_value(payload).context(SerializeEventSnafu),
            None => Ok(serde_json::Value::Null),
        }
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        ReconciliationLocator::schema()
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        Ok(self
            .locators
            .iter()
            .map(ReconciliationLocator::row)
            .collect())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn resolve_strategy_name(strategy: ResolveStrategy) -> &'static str {
    match strategy {
        ResolveStrategy::UseLatest => "use_latest",
        ResolveStrategy::UseMetasrv => "use_metasrv",
        ResolveStrategy::AbortOnConflict => "abort_on_conflict",
    }
}

fn nullable_table_id(value: Option<TableId>) -> api::v1::Value {
    nullable_value(value.map(ValueData::U32Value))
}

#[cfg(test)]
mod tests {
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Row, SemanticType, Value};
    use common_event_recorder::Event;
    use serde_json::json;

    use super::*;

    #[test]
    fn reconciliation_events_use_the_shared_locator_contract() {
        let table = ReconcileTableEvent::table_lifecycle(ReconciliationLocator::physical_table(
            "greptime", "public", "metrics", 42,
        ));
        assert_eq!(table.event_type(), RECONCILE_TABLE_EVENT_TYPE);
        assert_eq!(
            table
                .extra_schema()
                .into_iter()
                .map(|column| {
                    (
                        column.column_name,
                        ColumnDataType::try_from(column.datatype).unwrap(),
                        SemanticType::try_from(column.semantic_type).unwrap(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (
                    "catalog_name".to_string(),
                    ColumnDataType::String,
                    SemanticType::Field
                ),
                (
                    "schema_name".to_string(),
                    ColumnDataType::String,
                    SemanticType::Field
                ),
                (
                    "table_name".to_string(),
                    ColumnDataType::String,
                    SemanticType::Field
                ),
                (
                    "table_id".to_string(),
                    ColumnDataType::Uint32,
                    SemanticType::Field
                ),
                (
                    "physical_table_id".to_string(),
                    ColumnDataType::Uint32,
                    SemanticType::Field,
                ),
            ]
        );
        assert_eq!(
            table.extra_rows().unwrap(),
            vec![Row {
                values: vec![
                    ValueData::StringValue("greptime".to_string()).into(),
                    ValueData::StringValue("public".to_string()).into(),
                    ValueData::StringValue("metrics".to_string()).into(),
                    ValueData::U32Value(42).into(),
                    Value::default(),
                ],
            }]
        );
        assert_eq!(table.json_payload().unwrap(), serde_json::Value::Null);

        let logical_tables = ReconcileLogicalTablesEvent::lifecycle(vec![
            ReconciliationLocator::logical_table("greptime", "public", "cpu", 43, 42),
            ReconciliationLocator::logical_table("greptime", "public", "memory", 44, 42),
        ]);
        assert_eq!(
            logical_tables.event_type(),
            RECONCILE_LOGICAL_TABLES_EVENT_TYPE
        );
        assert_eq!(logical_tables.extra_schema(), table.extra_schema());
        assert_eq!(
            logical_tables.extra_rows().unwrap(),
            vec![
                Row {
                    values: vec![
                        ValueData::StringValue("greptime".to_string()).into(),
                        ValueData::StringValue("public".to_string()).into(),
                        ValueData::StringValue("cpu".to_string()).into(),
                        ValueData::U32Value(43).into(),
                        ValueData::U32Value(42).into(),
                    ],
                },
                Row {
                    values: vec![
                        ValueData::StringValue("greptime".to_string()).into(),
                        ValueData::StringValue("public".to_string()).into(),
                        ValueData::StringValue("memory".to_string()).into(),
                        ValueData::U32Value(44).into(),
                        ValueData::U32Value(42).into(),
                    ],
                },
            ]
        );
        assert_eq!(
            logical_tables.json_payload().unwrap(),
            serde_json::Value::Null
        );
    }

    #[test]
    fn submitted_payloads_are_versioned_and_use_stable_strategy_names() {
        for (strategy, expected) in [
            (ResolveStrategy::UseLatest, "use_latest"),
            (ResolveStrategy::UseMetasrv, "use_metasrv"),
            (ResolveStrategy::AbortOnConflict, "abort_on_conflict"),
        ] {
            let table = ReconcileTableEvent::table_submitted(
                ReconciliationLocator::physical_table("greptime", "public", "metrics", 42),
                strategy,
                true,
            );
            assert_eq!(
                table.json_payload().unwrap(),
                json!({
                    "version": 1,
                    "resolve_strategy": expected,
                    "is_subprocedure": true,
                })
            );
        }

        let logical_tables = ReconcileLogicalTablesEvent::submitted(
            vec![
                ReconciliationLocator::logical_table("greptime", "public", "cpu", 43, 42),
                ReconciliationLocator::logical_table("greptime", "public", "memory", 44, 42),
            ],
            true,
        );
        assert_eq!(
            logical_tables.json_payload().unwrap(),
            json!({
                "version": 1,
                "logical_table_count": 2,
                "is_subprocedure": true,
            })
        );
    }

    #[test]
    fn terminal_payloads_distinguish_complete_and_partial_results() {
        let table = ReconcileTableEvent::table_result(
            ReconciliationLocator::physical_table("greptime", "public", "metrics", 42),
            false,
            Some("inconsistent"),
            Some(ResolveStrategy::UseMetasrv),
            Some(4),
            3,
            2,
            true,
            Some("update_table_info"),
        );
        assert_eq!(
            table.json_payload().unwrap(),
            json!({
                "version": 1,
                "complete": false,
                "metadata_state": "inconsistent",
                "resolution_strategy_applied": "use_metasrv",
                "resolved_column_count": 4,
                "scanned_region_count": 3,
                "updated_region_count": 2,
                "table_info_updated": true,
                "last_completed_phase": "update_table_info",
            })
        );

        let logical_tables = ReconcileLogicalTablesEvent::result(
            vec![ReconciliationLocator::logical_table(
                "greptime", "public", "cpu", 43, 42,
            )],
            true,
            3,
            1,
            2,
            12,
            18,
            2,
            6,
            1,
            Some("update_table_infos"),
        );
        assert_eq!(
            logical_tables.json_payload().unwrap(),
            json!({
                "version": 1,
                "complete": true,
                "processed_table_count": 6,
                "metadata_consistent_table_count": 3,
                "metadata_inconsistent_table_count": 1,
                "missing_region_table_count": 2,
                "resolved_column_count": 12,
                "scanned_region_count": 18,
                "created_region_table_count": 2,
                "created_region_count": 6,
                "updated_table_info_count": 1,
                "last_completed_phase": "update_table_infos",
            })
        );
    }
}
