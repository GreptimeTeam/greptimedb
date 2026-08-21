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

pub(crate) const RECONCILE_CATALOG_EVENT_TYPE: &str = "reconcile_catalog";
pub(crate) const RECONCILE_DATABASE_EVENT_TYPE: &str = "reconcile_database";
pub(crate) const RECONCILE_TABLE_EVENT_TYPE: &str = "reconcile_table";
const PAYLOAD_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy)]
enum ReconciliationEventType {
    Catalog,
    Database,
    Table,
}

impl ReconciliationEventType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => RECONCILE_CATALOG_EVENT_TYPE,
            Self::Database => RECONCILE_DATABASE_EVENT_TYPE,
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
    pub(crate) fn catalog(catalog_name: &str) -> Self {
        Self {
            catalog_name: Some(catalog_name.to_string()),
            ..Default::default()
        }
    }

    pub(crate) fn database(catalog_name: &str, schema_name: &str) -> Self {
        Self {
            catalog_name: Some(catalog_name.to_string()),
            schema_name: Some(schema_name.to_string()),
            ..Default::default()
        }
    }

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
    CatalogSubmitted(CatalogSubmittedPayload),
    CatalogResult(CatalogResultPayload),
    DatabaseSubmitted(DatabaseSubmittedPayload),
    DatabaseResult(DatabaseResultPayload),
    TableSubmitted(TableSubmittedPayload),
    TableResult(TableResultPayload),
}

#[derive(Debug, Serialize)]
struct CatalogSubmittedPayload {
    version: u8,
    resolve_strategy: &'static str,
    fail_fast: bool,
    parallelism: usize,
}

#[derive(Debug, Serialize)]
struct CatalogResultPayload {
    version: u8,
    complete: bool,
    processed_database_count: usize,
    succeeded_database_count: usize,
    failed_database_count: usize,
    last_completed_phase: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct DatabaseSubmittedPayload {
    version: u8,
    resolve_strategy: &'static str,
    fail_fast: bool,
    parallelism: usize,
    is_subprocedure: bool,
}

#[derive(Debug, Serialize)]
struct DatabaseResultPayload {
    version: u8,
    complete: bool,
    processed_table_count: usize,
    succeeded_table_count: usize,
    failed_table_count: usize,
    succeeded_subprocedure_count: usize,
    failed_subprocedure_count: usize,
    last_completed_phase: Option<&'static str>,
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
    pub(crate) fn catalog_submitted(
        locator: ReconciliationLocator,
        resolve_strategy: ResolveStrategy,
        fail_fast: bool,
        parallelism: usize,
    ) -> Self {
        Self {
            event_type: ReconciliationEventType::Catalog,
            locator,
            payload: Some(ReconciliationPayload::CatalogSubmitted(
                CatalogSubmittedPayload {
                    version: PAYLOAD_VERSION,
                    resolve_strategy: resolve_strategy_name(resolve_strategy),
                    fail_fast,
                    parallelism,
                },
            )),
        }
    }

    pub(crate) fn catalog_result(
        locator: ReconciliationLocator,
        complete: bool,
        succeeded_database_count: usize,
        failed_database_count: usize,
        last_completed_phase: Option<&'static str>,
    ) -> Self {
        Self {
            event_type: ReconciliationEventType::Catalog,
            locator,
            payload: Some(ReconciliationPayload::CatalogResult(CatalogResultPayload {
                version: PAYLOAD_VERSION,
                complete,
                processed_database_count: succeeded_database_count + failed_database_count,
                succeeded_database_count,
                failed_database_count,
                last_completed_phase,
            })),
        }
    }

    pub(crate) fn catalog_lifecycle(locator: ReconciliationLocator) -> Self {
        Self::lifecycle(ReconciliationEventType::Catalog, locator)
    }

    pub(crate) fn database_submitted(
        locator: ReconciliationLocator,
        resolve_strategy: ResolveStrategy,
        fail_fast: bool,
        parallelism: usize,
        is_subprocedure: bool,
    ) -> Self {
        Self {
            event_type: ReconciliationEventType::Database,
            locator,
            payload: Some(ReconciliationPayload::DatabaseSubmitted(
                DatabaseSubmittedPayload {
                    version: PAYLOAD_VERSION,
                    resolve_strategy: resolve_strategy_name(resolve_strategy),
                    fail_fast,
                    parallelism,
                    is_subprocedure,
                },
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn database_result(
        locator: ReconciliationLocator,
        complete: bool,
        succeeded_table_count: usize,
        failed_table_count: usize,
        succeeded_subprocedure_count: usize,
        failed_subprocedure_count: usize,
        last_completed_phase: Option<&'static str>,
    ) -> Self {
        Self {
            event_type: ReconciliationEventType::Database,
            locator,
            payload: Some(ReconciliationPayload::DatabaseResult(
                DatabaseResultPayload {
                    version: PAYLOAD_VERSION,
                    complete,
                    processed_table_count: succeeded_table_count + failed_table_count,
                    succeeded_table_count,
                    failed_table_count,
                    succeeded_subprocedure_count,
                    failed_subprocedure_count,
                    last_completed_phase,
                },
            )),
        }
    }

    pub(crate) fn database_lifecycle(locator: ReconciliationLocator) -> Self {
        Self::lifecycle(ReconciliationEventType::Database, locator)
    }

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
        let catalog = ReconciliationEvent::catalog_submitted(
            ReconciliationLocator::catalog("greptime"),
            ResolveStrategy::UseLatest,
            false,
            32,
        );
        assert_eq!(catalog.event_type(), RECONCILE_CATALOG_EVENT_TYPE);
        assert_eq!(
            catalog
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
            catalog.extra_rows().unwrap(),
            vec![Row {
                values: vec![
                    ValueData::StringValue("greptime".to_string()).into(),
                    Value::default(),
                    Value::default(),
                    Value::default(),
                    Value::default(),
                ],
            }]
        );

        let database = ReconciliationEvent::database_lifecycle(ReconciliationLocator::database(
            "greptime", "public",
        ));
        assert_eq!(database.event_type(), RECONCILE_DATABASE_EVENT_TYPE);
        assert_eq!(database.extra_schema(), catalog.extra_schema());
        assert_eq!(
            database.extra_rows().unwrap(),
            vec![Row {
                values: vec![
                    ValueData::StringValue("greptime".to_string()).into(),
                    ValueData::StringValue("public".to_string()).into(),
                    Value::default(),
                    Value::default(),
                    Value::default(),
                ],
            }]
        );
        assert_eq!(database.json_payload().unwrap(), serde_json::Value::Null);

        let table = ReconciliationEvent::table_lifecycle(ReconciliationLocator::physical_table(
            "greptime", "public", "metrics", 42,
        ));
        assert_eq!(table.event_type(), RECONCILE_TABLE_EVENT_TYPE);
        assert_eq!(table.extra_schema(), catalog.extra_schema());
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
            let catalog = ReconciliationEvent::catalog_submitted(
                ReconciliationLocator::catalog("greptime"),
                strategy,
                true,
                16,
            );
            assert_eq!(
                catalog.json_payload().unwrap(),
                json!({
                    "version": 1,
                    "resolve_strategy": expected,
                    "fail_fast": true,
                    "parallelism": 16,
                })
            );
        }

        let database = ReconciliationEvent::database_submitted(
            ReconciliationLocator::database("greptime", "public"),
            ResolveStrategy::UseMetasrv,
            false,
            64,
            true,
        );
        assert_eq!(
            database.json_payload().unwrap(),
            json!({
                "version": 1,
                "resolve_strategy": "use_metasrv",
                "fail_fast": false,
                "parallelism": 64,
                "is_subprocedure": true,
            })
        );

        let table = ReconciliationEvent::table_submitted(
            ReconciliationLocator::physical_table("greptime", "public", "metrics", 42),
            ResolveStrategy::AbortOnConflict,
            true,
        );
        assert_eq!(
            table.json_payload().unwrap(),
            json!({
                "version": 1,
                "resolve_strategy": "abort_on_conflict",
                "is_subprocedure": true,
            })
        );
    }

    #[test]
    fn terminal_payloads_distinguish_complete_and_partial_results() {
        let catalog = ReconciliationEvent::catalog_result(
            ReconciliationLocator::catalog("greptime"),
            true,
            3,
            1,
            Some("databases"),
        );
        assert_eq!(
            catalog.json_payload().unwrap(),
            json!({
                "version": 1,
                "complete": true,
                "processed_database_count": 4,
                "succeeded_database_count": 3,
                "failed_database_count": 1,
                "last_completed_phase": "databases",
            })
        );

        let database = ReconciliationEvent::database_result(
            ReconciliationLocator::database("greptime", "public"),
            false,
            5,
            2,
            4,
            1,
            Some("physical_tables"),
        );
        assert_eq!(
            database.json_payload().unwrap(),
            json!({
                "version": 1,
                "complete": false,
                "processed_table_count": 7,
                "succeeded_table_count": 5,
                "failed_table_count": 2,
                "succeeded_subprocedure_count": 4,
                "failed_subprocedure_count": 1,
                "last_completed_phase": "physical_tables",
            })
        );

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
