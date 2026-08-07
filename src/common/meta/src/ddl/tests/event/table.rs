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

use api::v1::alter_table_expr::Kind as AlterTableKind;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Repartition, SemanticType, Value};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN, EVENT_CONTEXT_COLUMN, PHYSICAL_TABLE_ID_COLUMN, SCHEMA_NAME_COLUMN,
    TABLE_ID_COLUMN, TABLE_NAME_COLUMN, jsonb_value,
};
use common_event_recorder::{Event, EventTypeFilter};
use common_procedure::{
    ChildSubmissionOutcome, EventRuntimeContext, EventTrigger, Procedure, ProcedureId,
    ProcedureState, RetryPhase,
};
use common_time::Timestamp;
use serde_json::{Value as JsonValue, json};

use super::test_util::assert_event_filter;
use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::ddl::alter_table::AlterTableProcedure;
use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::create_table::CreateTableProcedure;
use crate::ddl::drop_table::DropTableProcedure;
use crate::ddl::event::table::{
    TABLE_DDL_PAYLOAD_VERSION, TableDdlEvent, TableDdlEventType, TableDdlLocator,
    alter_table_kind_name,
};
#[cfg(feature = "enterprise")]
use crate::ddl::purge_dropped_table::PurgeDroppedTableProcedure;
use crate::ddl::test_util::create_table::test_create_table_task as test_create_table_task_with_id;
use crate::ddl::test_util::test_create_logical_table_task;
use crate::ddl::tests::alter_logical_tables::make_alter_logical_table_add_column_task;
use crate::ddl::tests::alter_table::test_alter_table_task;
use crate::ddl::tests::create_table::test_create_table_task;
use crate::ddl::truncate_table::TruncateTableProcedure;
#[cfg(feature = "enterprise")]
use crate::ddl::undrop_table::UndropTableProcedure;
use crate::key::DeserializedValueWithBytes;
use crate::key::table_info::TableInfoValue;
use crate::rpc::ddl::{DropTableTask, EventContext, QueryContext, TruncateTableTask};
#[cfg(feature = "enterprise")]
use crate::rpc::ddl::{PurgeDroppedTableTask, TriggerReason, UndropTableTask};
use crate::test_util::{MockDatanodeManager, new_ddl_context};

struct EventCase {
    event_type: TableDdlEventType,
    event: TableDdlEvent,
    payload: JsonValue,
    rows: Vec<Vec<Value>>,
}

struct ProcedureCase {
    procedure: Box<dyn Procedure>,
    event_type: &'static str,
    payload: JsonValue,
    rows: Vec<Vec<Value>>,
}

#[test]
fn repartition_kind_is_not_supported_by_alter_table_events() {
    assert_eq!(
        None,
        alter_table_kind_name(&AlterTableKind::Repartition(Repartition::default()))
    );
}

#[test]
fn submitted_event_contracts_are_bounded_and_fixed() {
    for case in event_cases() {
        assert_eq!(case.event.event_type(), case.event_type.as_str());
        assert_eq!(case.event.json_payload().unwrap(), case.payload);
        assert_eq!(
            case.event
                .extra_rows()
                .unwrap()
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>(),
            case.rows
        );

        let schema = case.event.extra_schema();
        assert_eq!(
            schema
                .iter()
                .map(|column| (column.column_name.as_str(), column.datatype))
                .collect::<Vec<_>>(),
            expected_schema(case.event_type)
        );
        assert!(
            schema
                .iter()
                .all(|column| column.semantic_type == SemanticType::Field as i32)
        );

        let lifecycle = TableDdlEvent::lifecycle(case.event_type);
        assert_eq!(lifecycle.extra_schema(), schema);
        assert_eq!(lifecycle.json_payload().unwrap(), JsonValue::Null);
        assert_eq!(
            lifecycle.extra_rows().unwrap()[0].values,
            vec![Value::default(); schema.len()]
        );
    }
}

#[test]
fn procedures_map_tasks_to_submitted_events() {
    for case in procedure_cases() {
        let event = event_for(case.procedure.as_ref(), EventTrigger::Submitted);

        assert_eq!(event.event_type(), case.event_type);
        assert_eq!(event.json_payload().unwrap(), case.payload);
        assert_eq!(
            event
                .extra_rows()
                .unwrap()
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>(),
            case.rows
        );
    }
}

#[test]
fn later_lifecycle_events_are_uniform() {
    let triggers = [
        EventTrigger::Recovered,
        EventTrigger::ChildSubmitted {
            procedure_id: ProcedureId::random(),
            outcome: ChildSubmissionOutcome::Accepted,
        },
        EventTrigger::Retrying {
            phase: RetryPhase::Execute,
            attempt: 1,
        },
        EventTrigger::RollingBack,
        EventTrigger::Succeeded,
        EventTrigger::Failed,
        EventTrigger::Poisoned,
    ];

    for case in procedure_cases() {
        let submitted = event_for(case.procedure.as_ref(), EventTrigger::Submitted);
        let schema = submitted.extra_schema();

        for trigger in &triggers {
            let event = event_for(case.procedure.as_ref(), trigger.clone());

            assert_eq!(event.event_type(), case.event_type);
            assert_eq!(event.extra_schema(), schema);
            assert_eq!(event.json_payload().unwrap(), JsonValue::Null);
            assert_eq!(
                event.extra_rows().unwrap()[0].values,
                vec![Value::default(); schema.len()]
            );
        }
    }
}

#[test]
fn create_success_events_keep_allocated_ids() {
    let mut task = test_create_table_task("create_success");
    task.table_info.ident.table_id = 7;
    let create_table = CreateTableProcedure::new(
        task,
        QueryContext::default(),
        EventContext::default(),
        test_context(),
    )
    .unwrap();
    let state = ProcedureState::Done {
        output: Some(Arc::new(42_u32)),
    };
    let event = event_for_state(&create_table, EventTrigger::Succeeded, &state);

    assert_eq!(event.event_type(), "create_table");
    assert_eq!(event.json_payload().unwrap(), JsonValue::Null);
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        table_locator_values(None, Some(42))
    );

    let logical_tables = CreateLogicalTablesProcedure::new(
        vec![
            test_create_logical_table_task("foo"),
            test_create_logical_table_task("bar"),
        ],
        1024,
        EventContext::default(),
        test_context(),
    );
    let state = ProcedureState::Done {
        output: Some(Arc::new(vec![1025_u32, 1026_u32])),
    };
    let event = event_for_state(&logical_tables, EventTrigger::Succeeded, &state);

    assert_eq!(event.event_type(), "create_logical_tables");
    assert_eq!(event.json_payload().unwrap(), JsonValue::Null);
    assert_eq!(
        event
            .extra_rows()
            .unwrap()
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        vec![
            logical_locator_values("foo", Some(1025), 1024),
            logical_locator_values("bar", Some(1026), 1024),
        ]
    );
}

#[test]
fn table_procedures_honor_event_type_filter() {
    for case in procedure_cases() {
        assert_event_filter(case.procedure.as_ref(), case.event_type);
    }
}

fn event_cases() -> Vec<EventCase> {
    vec![
        EventCase {
            event_type: TableDdlEventType::CreateTable,
            event: TableDdlEvent::create_table_submitted(
                TableDdlLocator::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "create"),
                true,
                "mito2",
                EventContext::default(),
            ),
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "create_if_not_exists": true,
                "engine": "mito2",
            }),
            rows: vec![submitted_table_locator_values(Some("create"), None)],
        },
        EventCase {
            event_type: TableDdlEventType::CreateLogicalTables,
            event: TableDdlEvent::create_logical_tables_submitted(
                [
                    TableDdlLocator::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "logical1")
                        .with_physical_table_id(10),
                    TableDdlLocator::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "logical2")
                        .with_physical_table_id(10),
                ],
                2,
                EventContext::default(),
            ),
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "table_count": 2,
            }),
            rows: vec![
                submitted_logical_locator_values("logical1", None, 10),
                submitted_logical_locator_values("logical2", None, 10),
            ],
        },
        EventCase {
            event_type: TableDdlEventType::AlterTable,
            event: TableDdlEvent::alter_table_submitted(
                TableDdlLocator::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "alter")
                    .with_table_id(11),
                Some("drop_columns"),
                EventContext::default(),
            ),
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "kind": "drop_columns",
            }),
            rows: vec![submitted_table_locator_values(Some("alter"), Some(11))],
        },
        EventCase {
            event_type: TableDdlEventType::AlterLogicalTables,
            event: TableDdlEvent::alter_logical_tables_submitted(
                [
                    TableDdlLocator::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "logical1")
                        .with_physical_table_id(10),
                    TableDdlLocator::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "logical2")
                        .with_physical_table_id(10),
                ],
                2,
                ["rename_table", "add_columns", "add_columns"],
                EventContext::default(),
            ),
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "table_count": 2,
                "kinds": ["add_columns", "rename_table"],
            }),
            rows: vec![
                submitted_logical_locator_values("logical1", None, 10),
                submitted_logical_locator_values("logical2", None, 10),
            ],
        },
        EventCase {
            event_type: TableDdlEventType::DropTable,
            event: TableDdlEvent::drop_table_submitted(
                TableDdlLocator::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "drop")
                    .with_table_id(12),
                true,
                EventContext::default(),
            ),
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "drop_if_exists": true,
            }),
            rows: vec![submitted_table_locator_values(Some("drop"), Some(12))],
        },
        #[cfg(feature = "enterprise")]
        EventCase {
            event_type: TableDdlEventType::UndropTable,
            event: TableDdlEvent::undrop_table_submitted(
                TableDdlLocator::from_table_id(13),
                EventContext::default(),
            ),
            payload: json!({"version": TABLE_DDL_PAYLOAD_VERSION}),
            rows: vec![submitted_table_locator_values(None, Some(13))],
        },
        #[cfg(feature = "enterprise")]
        EventCase {
            event_type: TableDdlEventType::PurgeDroppedTable,
            event: TableDdlEvent::purge_dropped_table_submitted(
                TableDdlLocator::from_table_id(14),
                EventContext::default(),
            ),
            payload: json!({"version": TABLE_DDL_PAYLOAD_VERSION}),
            rows: vec![submitted_table_locator_values(None, Some(14))],
        },
        EventCase {
            event_type: TableDdlEventType::TruncateTable,
            event: TableDdlEvent::truncate_table_submitted(
                TableDdlLocator::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "truncate")
                    .with_table_id(15),
                4,
                EventContext::default(),
            ),
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "time_range_count": 4,
            }),
            rows: vec![submitted_table_locator_values(Some("truncate"), Some(15))],
        },
    ]
}

fn procedure_cases() -> Vec<ProcedureCase> {
    let create_table = CreateTableProcedure::new(
        test_create_table_task("create"),
        QueryContext::default(),
        EventContext::default(),
        test_context(),
    )
    .unwrap();
    let create_logical_tables = CreateLogicalTablesProcedure::new(
        vec![
            test_create_logical_table_task("logical1"),
            test_create_logical_table_task("logical2"),
        ],
        41,
        EventContext::default(),
        test_context(),
    );
    let alter_table = AlterTableProcedure::new(
        42,
        test_alter_table_task("alter"),
        EventContext::default(),
        test_context(),
    )
    .unwrap();
    let alter_logical_tables = AlterLogicalTablesProcedure::new(
        vec![
            make_alter_logical_table_add_column_task(
                Some(DEFAULT_SCHEMA_NAME),
                "logical1",
                vec!["tag1".to_string()],
            ),
            make_alter_logical_table_add_column_task(
                Some(DEFAULT_SCHEMA_NAME),
                "logical2",
                vec!["tag2".to_string()],
            ),
        ],
        43,
        EventContext::default(),
        test_context(),
    );
    let drop_table = DropTableProcedure::new(
        DropTableTask {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table: "drop".to_string(),
            table_id: 44,
            drop_if_exists: true,
        },
        test_context(),
        EventContext::default(),
    );
    #[cfg(feature = "enterprise")]
    let undrop_table = UndropTableProcedure::new(
        UndropTableTask { table_id: 45 },
        test_context(),
        EventContext::default(),
    );
    #[cfg(feature = "enterprise")]
    let purge_dropped_table = PurgeDroppedTableProcedure::new_if_expired(
        PurgeDroppedTableTask { table_id: 46 },
        test_context(),
        EventContext::new(TriggerReason::ScheduledGc),
    );
    let truncate_table = truncate_procedure(TruncateTableTask {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table: "truncate".to_string(),
        table_id: 47,
        time_ranges: vec![(
            Timestamp::new_millisecond(1_000),
            Timestamp::new_millisecond(2_000),
        )],
    });

    vec![
        ProcedureCase {
            procedure: Box::new(create_table),
            event_type: "create_table",
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "create_if_not_exists": false,
                "engine": "mito2",
            }),
            rows: vec![submitted_table_locator_values(Some("create"), None)],
        },
        ProcedureCase {
            procedure: Box::new(create_logical_tables),
            event_type: "create_logical_tables",
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "table_count": 2,
            }),
            rows: vec![
                submitted_logical_locator_values("logical1", None, 41),
                submitted_logical_locator_values("logical2", None, 41),
            ],
        },
        ProcedureCase {
            procedure: Box::new(alter_table),
            event_type: "alter_table",
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "kind": "drop_columns",
            }),
            rows: vec![submitted_table_locator_values(Some("alter"), Some(42))],
        },
        ProcedureCase {
            procedure: Box::new(alter_logical_tables),
            event_type: "alter_logical_tables",
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "table_count": 2,
                "kinds": ["add_columns"],
            }),
            rows: vec![
                submitted_logical_locator_values("logical1", None, 43),
                submitted_logical_locator_values("logical2", None, 43),
            ],
        },
        ProcedureCase {
            procedure: Box::new(drop_table),
            event_type: "drop_table",
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "drop_if_exists": true,
            }),
            rows: vec![submitted_table_locator_values(Some("drop"), Some(44))],
        },
        #[cfg(feature = "enterprise")]
        ProcedureCase {
            procedure: Box::new(undrop_table),
            event_type: "undrop_table",
            payload: json!({"version": TABLE_DDL_PAYLOAD_VERSION}),
            rows: vec![submitted_table_locator_values(None, Some(45))],
        },
        #[cfg(feature = "enterprise")]
        ProcedureCase {
            procedure: Box::new(purge_dropped_table),
            event_type: "purge_dropped_table",
            payload: json!({"version": TABLE_DDL_PAYLOAD_VERSION}),
            rows: vec![submitted_table_locator_values_with_event_context(
                None,
                Some(46),
                EventContext::new(TriggerReason::ScheduledGc),
            )],
        },
        ProcedureCase {
            procedure: Box::new(truncate_table),
            event_type: "truncate_table",
            payload: json!({
                "version": TABLE_DDL_PAYLOAD_VERSION,
                "time_range_count": 1,
            }),
            rows: vec![submitted_table_locator_values(Some("truncate"), Some(47))],
        },
    ]
}

fn expected_schema(event_type: TableDdlEventType) -> Vec<(&'static str, i32)> {
    let mut schema = vec![
        (CATALOG_NAME_COLUMN.name(), ColumnDataType::String as i32),
        (SCHEMA_NAME_COLUMN.name(), ColumnDataType::String as i32),
        (TABLE_NAME_COLUMN.name(), ColumnDataType::String as i32),
        (TABLE_ID_COLUMN.name(), ColumnDataType::Uint32 as i32),
    ];
    if matches!(
        event_type,
        TableDdlEventType::CreateLogicalTables | TableDdlEventType::AlterLogicalTables
    ) {
        schema.push((
            PHYSICAL_TABLE_ID_COLUMN.name(),
            ColumnDataType::Uint32 as i32,
        ));
    }
    schema.push((EVENT_CONTEXT_COLUMN.name(), ColumnDataType::Binary as i32));
    schema
}

fn table_locator_values(table_name: Option<&str>, table_id: Option<u32>) -> Vec<Value> {
    let (catalog_name, schema_name) = if table_name.is_some() {
        (
            string_value(DEFAULT_CATALOG_NAME),
            string_value(DEFAULT_SCHEMA_NAME),
        )
    } else {
        (Value::default(), Value::default())
    };
    let mut values = vec![
        catalog_name,
        schema_name,
        table_name.map(string_value).unwrap_or_default(),
        table_id.map(table_id_value).unwrap_or_default(),
    ];
    values.push(Value::default());
    values
}

fn logical_locator_values(
    table_name: &str,
    table_id: Option<u32>,
    physical_table_id: u32,
) -> Vec<Value> {
    let mut values = table_locator_values(Some(table_name), table_id);
    let event_context = values.pop().unwrap();
    values.push(table_id_value(physical_table_id));
    values.push(event_context);
    values
}

fn submitted_table_locator_values(table_name: Option<&str>, table_id: Option<u32>) -> Vec<Value> {
    submitted_table_locator_values_with_event_context(table_name, table_id, EventContext::default())
}

fn submitted_table_locator_values_with_event_context(
    table_name: Option<&str>,
    table_id: Option<u32>,
    event_context: EventContext,
) -> Vec<Value> {
    let mut values = table_locator_values(table_name, table_id);
    *values.last_mut().unwrap() = event_context_value_for(event_context);
    values
}

fn submitted_logical_locator_values(
    table_name: &str,
    table_id: Option<u32>,
    physical_table_id: u32,
) -> Vec<Value> {
    let mut values = logical_locator_values(table_name, table_id, physical_table_id);
    *values.last_mut().unwrap() = event_context_value();
    values
}

fn event_context_value() -> Value {
    event_context_value_for(EventContext::default())
}

fn event_context_value_for(event_context: EventContext) -> Value {
    jsonb_value(&serde_json::to_value(event_context).unwrap())
}

fn string_value(value: &str) -> Value {
    ValueData::StringValue(value.to_string()).into()
}

fn table_id_value(value: u32) -> Value {
    ValueData::U32Value(value).into()
}

fn truncate_procedure(task: TruncateTableTask) -> TruncateTableProcedure {
    let table_info = test_create_table_task_with_id("metrics", task.table_id).table_info;
    TruncateTableProcedure::new(
        task,
        DeserializedValueWithBytes::from_inner(TableInfoValue::new(table_info)),
        test_context(),
        EventContext::default(),
    )
}

fn test_context() -> crate::ddl::DdlContext {
    new_ddl_context(Arc::new(MockDatanodeManager::new(())))
}

fn event_for(procedure: &dyn Procedure, trigger: EventTrigger) -> Box<dyn Event> {
    event_for_state(procedure, trigger, &ProcedureState::Running)
}

fn event_for_state(
    procedure: &dyn Procedure,
    trigger: EventTrigger,
    lifecycle_state: &ProcedureState,
) -> Box<dyn Event> {
    procedure
        .event(&EventRuntimeContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state,
            trigger,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap()
}
