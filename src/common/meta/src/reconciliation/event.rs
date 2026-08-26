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

/// Stable event type stored for physical table reconciliation procedures.
pub(crate) const RECONCILE_TABLE_EVENT_TYPE: &str = "reconcile_table";
const PAYLOAD_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy)]
enum ReconciliationEventType {
    Table,
}

impl ReconciliationEventType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Table => RECONCILE_TABLE_EVENT_TYPE,
        }
    }
}

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
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ReconciliationPayload {
    TableSubmitted(TableSubmittedPayload),
    TableResult(TableResultPayload),
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

/// Event representation shared by reconciliation procedures.
#[derive(Debug)]
pub(crate) struct ReconciliationEvent {
    event_type: ReconciliationEventType,
    locator: ReconciliationLocator,
    payload: Option<ReconciliationPayload>,
}

impl ReconciliationEvent {
    /// Builds the bounded intent event emitted when table reconciliation is submitted.
    pub(crate) fn table_submitted(
        locator: ReconciliationLocator,
        resolve_strategy: ResolveStrategy,
        is_subprocedure: bool,
    ) -> Self {
        Self {
            event_type: ReconciliationEventType::Table,
            locator,
            payload: Some(ReconciliationPayload::TableSubmitted(
                TableSubmittedPayload {
                    version: PAYLOAD_VERSION,
                    resolve_strategy: resolve_strategy_name(resolve_strategy),
                    is_subprocedure,
                },
            )),
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
            event_type: ReconciliationEventType::Table,
            locator,
            payload: Some(ReconciliationPayload::TableResult(TableResultPayload {
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
        Self::lifecycle(ReconciliationEventType::Table, locator)
    }

    fn lifecycle(event_type: ReconciliationEventType, locator: ReconciliationLocator) -> Self {
        Self {
            event_type,
            locator,
            payload: None,
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
}

impl Event for ReconciliationEvent {
    fn event_type(&self) -> &str {
        self.event_type.as_str()
    }

    fn json_payload(&self) -> Result<serde_json::Value> {
        match &self.payload {
            Some(payload) => serde_json::to_value(payload).context(SerializeEventSnafu),
            None => Ok(serde_json::Value::Null),
        }
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        Self::schema()
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        Ok(vec![Row {
            values: vec![
                nullable_string(self.locator.catalog_name.as_deref()),
                nullable_string(self.locator.schema_name.as_deref()),
                nullable_string(self.locator.table_name.as_deref()),
                nullable_table_id(self.locator.table_id),
                nullable_table_id(self.locator.physical_table_id),
            ],
        }])
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
        let table = ReconciliationEvent::table_lifecycle(ReconciliationLocator::physical_table(
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
                    SemanticType::Field,
                ),
                (
                    "schema_name".to_string(),
                    ColumnDataType::String,
                    SemanticType::Field,
                ),
                (
                    "table_name".to_string(),
                    ColumnDataType::String,
                    SemanticType::Field,
                ),
                (
                    "table_id".to_string(),
                    ColumnDataType::Uint32,
                    SemanticType::Field,
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
    }

    #[test]
    fn submitted_payloads_are_versioned_and_use_stable_strategy_names() {
        for (strategy, expected) in [
            (ResolveStrategy::UseLatest, "use_latest"),
            (ResolveStrategy::UseMetasrv, "use_metasrv"),
            (ResolveStrategy::AbortOnConflict, "abort_on_conflict"),
        ] {
            let table = ReconciliationEvent::table_submitted(
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
    }

    #[test]
    fn terminal_payloads_distinguish_complete_and_partial_results() {
        let table = ReconciliationEvent::table_result(
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
    }
}
