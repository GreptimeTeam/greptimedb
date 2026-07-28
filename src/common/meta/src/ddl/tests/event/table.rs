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

use std::collections::HashSet;
use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, SemanticType, Value};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN, PHYSICAL_TABLE_ID_COLUMN, SCHEMA_NAME_COLUMN, TABLE_ID_COLUMN,
    TABLE_NAME_COLUMN,
};
use common_event_recorder::testing::assert_event_contract;
use common_event_recorder::{Event, EventTypeFilter};
use common_procedure::{
    ChildSubmissionOutcome, EventContext, EventTrigger, Procedure, ProcedureId, ProcedureState,
    RetryPhase,
};
use common_time::Timestamp;
use serde::Serialize;
use serde_json::json;

use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::ddl::alter_table::AlterTableProcedure;
use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::create_table::CreateTableProcedure;
use crate::ddl::drop_table::DropTableProcedure;
use crate::ddl::event::table::{
    TABLE_DDL_PAYLOAD_VERSION, TableDdlEvent, TableDdlEventType, TableDdlLocator,
    versioned_table_ddl_payload, versioned_table_ddl_payload_or_error,
};
use crate::ddl::purge_dropped_table::PurgeDroppedTableProcedure;
use crate::ddl::test_util::create_table::test_create_table_task as test_create_table_task_with_id;
use crate::ddl::test_util::test_create_logical_table_task;
use crate::ddl::tests::alter_logical_tables::make_alter_logical_table_add_column_task;
use crate::ddl::tests::alter_table::test_alter_table_task;
use crate::ddl::tests::create_table::test_create_table_task;
use crate::ddl::truncate_table::TruncateTableProcedure;
use crate::ddl::undrop_table::UndropTableProcedure;
use crate::key::DeserializedValueWithBytes;
use crate::key::table_info::TableInfoValue;
use crate::rpc::ddl::{
    AlterTableTask, DropTableTask, PurgeDroppedTableTask, TruncateTableTask, UndropTableTask,
};
use crate::test_util::{MockDatanodeManager, new_ddl_context};

const ALL_EVENT_TYPES: [TableDdlEventType; 8] = [
    TableDdlEventType::CreateTable,
    TableDdlEventType::CreateLogicalTables,
    TableDdlEventType::AlterTable,
    TableDdlEventType::AlterLogicalTables,
    TableDdlEventType::DropTable,
    TableDdlEventType::UndropTable,
    TableDdlEventType::PurgeDroppedTable,
    TableDdlEventType::TruncateTable,
];

#[test]
fn table_ddl_event_types_have_fixed_field_schemas() {
    for event_type in ALL_EVENT_TYPES {
        let submitted = TableDdlEvent::submitted(
            event_type,
            TableDdlLocator::default(),
            json!({"version": 1}),
        );
        let lifecycle = TableDdlEvent::lifecycle(event_type);
        let schema = submitted.extra_schema();

        assert_eq!(schema, lifecycle.extra_schema());
        assert!(
            schema
                .iter()
                .all(|column| column.semantic_type == SemanticType::Field as i32)
        );
        assert_eq!(
            schema
                .iter()
                .map(|column| (column.column_name.as_str(), column.datatype))
                .collect::<Vec<_>>(),
            expected_schema(event_type)
        );
    }
}

#[test]
fn submitted_event_keeps_locator_and_full_versioned_payload() {
    let large_option = "x".repeat(128 * 1024);
    let payload = versioned_table_ddl_payload(json!({
        "kind": "create_table",
        "table_options": {"large": large_option},
    }))
    .unwrap();
    let event = TableDdlEvent::submitted(
        TableDdlEventType::CreateTable,
        TableDdlLocator::new("greptime", "public", "metrics").with_table_id(42),
        payload.clone(),
    );

    assert_eq!(event.json_payload().unwrap(), payload);
    assert_eq!(
        event.json_payload().unwrap()["version"],
        TABLE_DDL_PAYLOAD_VERSION
    );
    assert_eq!(
        event.json_payload().unwrap()["data"]["table_options"]["large"]
            .as_str()
            .unwrap()
            .len(),
        128 * 1024
    );
    let expected_rows = vec![api::v1::Row {
        values: vec![
            ValueData::StringValue("greptime".to_string()).into(),
            ValueData::StringValue("public".to_string()).into(),
            ValueData::StringValue("metrics".to_string()).into(),
            ValueData::U32Value(42).into(),
        ],
    }];
    assert_event_contract(
        &event,
        "create_table",
        &event.extra_schema(),
        &expected_rows,
    );
}

#[test]
fn submitted_event_keeps_locator_when_payload_serialization_fails() {
    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("sensitive serializer detail"))
        }
    }

    let payload = versioned_table_ddl_payload_or_error(Unserializable);
    let event = TableDdlEvent::submitted(
        TableDdlEventType::CreateTable,
        TableDdlLocator::new("greptime", "public", "metrics"),
        payload,
    );

    assert_eq!(
        event.json_payload().unwrap(),
        json!({
            "version": TABLE_DDL_PAYLOAD_VERSION,
            "error": {"code": "payload_serialization_failed"}
        })
    );
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![
            ValueData::StringValue("greptime".to_string()).into(),
            ValueData::StringValue("public".to_string()).into(),
            ValueData::StringValue("metrics".to_string()).into(),
            Value::default(),
        ]
    );
}

#[test]
fn lifecycle_event_has_one_null_row_and_null_payload() {
    let event = TableDdlEvent::lifecycle(TableDdlEventType::AlterLogicalTables);

    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![Value::default(); 5]
    );
}

#[test]
fn create_success_events_include_allocated_locators() {
    let create_table = TableDdlEvent::create_table_succeeded(7);
    assert_eq!(
        create_table.json_payload().unwrap(),
        serde_json::Value::Null
    );
    assert_eq!(
        create_table.extra_rows().unwrap()[0].values,
        vec![
            Value::default(),
            Value::default(),
            Value::default(),
            ValueData::U32Value(7).into(),
        ]
    );

    let logical_tables = TableDdlEvent::create_logical_tables_succeeded([
        TableDdlLocator::new("greptime", "public", "foo")
            .with_table_id(8)
            .with_physical_table_id(7),
        TableDdlLocator::new("greptime", "public", "bar")
            .with_table_id(9)
            .with_physical_table_id(7),
    ]);
    let rows = logical_tables.extra_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values[2],
        ValueData::StringValue("foo".to_string()).into()
    );
    assert_eq!(rows[0].values[3], ValueData::U32Value(8).into());
    assert_eq!(rows[1].values[3], ValueData::U32Value(9).into());
    assert_eq!(rows[0].values[4], ValueData::U32Value(7).into());
    assert_eq!(rows[1].values[4], ValueData::U32Value(7).into());
}

#[test]
fn logical_table_submission_emits_one_row_per_locator() {
    let locators = [
        TableDdlLocator::new("greptime", "public", "cpu")
            .with_table_id(10)
            .with_physical_table_id(1),
        TableDdlLocator::new("greptime", "public", "memory")
            .with_table_id(11)
            .with_physical_table_id(1),
    ];
    let event = TableDdlEvent::submitted_for_tables(
        TableDdlEventType::CreateLogicalTables,
        locators,
        versioned_table_ddl_payload(json!({"kind": "create_logical_tables"})).unwrap(),
    );

    let rows = event.extra_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values[2],
        ValueData::StringValue("cpu".to_string()).into()
    );
    assert_eq!(rows[0].values[4], ValueData::U32Value(1).into());
    assert_eq!(
        rows[1].values[2],
        ValueData::StringValue("memory".to_string()).into()
    );
    assert_eq!(rows[1].values[4], ValueData::U32Value(1).into());
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
    schema
}
#[test]
fn test_create_table_submitted_event_has_rich_payload_and_locators() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let task = test_create_table_task("foo");
    let expected_task = serde_json::to_value(&task).unwrap();
    let expected_table_info = serde_json::to_value(&task.table_info).unwrap();
    let expected_table_options = serde_json::to_value(&task.table_info.meta.options).unwrap();
    let procedure = CreateTableProcedure::new(task, new_ddl_context(node_manager)).unwrap();

    let state = ProcedureState::Running;

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &state,
            trigger: EventTrigger::Submitted,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();

    assert_eq!(event.event_type(), "create_table");
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![
            ValueData::StringValue("greptime".to_string()).into(),
            ValueData::StringValue("public".to_string()).into(),
            ValueData::StringValue("foo".to_string()).into(),
            Value::default(),
        ]
    );
    let payload = event.json_payload().unwrap();
    assert_eq!(payload["version"], 1);
    assert_eq!(payload["data"]["kind"], "create_table");
    assert_eq!(payload["data"]["task"], expected_task);
    assert_eq!(payload["data"]["table_info"], expected_table_info);
    assert_eq!(payload["data"]["table_options"], expected_table_options);
    assert!(payload["data"]["region_wal_options"].is_null());
}

#[test]
fn test_create_table_non_success_lifecycle_event_is_lightweight() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let procedure =
        CreateTableProcedure::new(test_create_table_task("foo"), new_ddl_context(node_manager))
            .unwrap();
    let state = ProcedureState::Running;

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &state,
            trigger: EventTrigger::Recovered,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();

    assert_eq!(event.event_type(), "create_table");
    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![Value::default(); 4]
    );
}

#[test]
fn test_create_table_succeeded_event_only_has_table_id() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let mut task = test_create_table_task("foo");
    task.table_info.ident.table_id = 7;
    let procedure = CreateTableProcedure::new(task, new_ddl_context(node_manager)).unwrap();
    let state = ProcedureState::Done {
        output: Some(Arc::new(42_u32)),
    };

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &state,
            trigger: EventTrigger::Succeeded,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();

    assert_eq!(event.event_type(), "create_table");
    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![
            Value::default(),
            Value::default(),
            Value::default(),
            ValueData::U32Value(42).into(),
        ]
    );
}

#[test]
fn test_submitted_event_has_one_rich_row_per_logical_table() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let mut foo = test_create_logical_table_task("foo");
    foo.set_table_id(7);
    let mut bar = test_create_logical_table_task("bar");
    bar.set_table_id(8);
    let expected_foo_task = serde_json::to_value(&foo).unwrap();
    let expected_bar_task = serde_json::to_value(&bar).unwrap();
    let procedure = CreateLogicalTablesProcedure::new(vec![foo, bar], 1024, ddl_context);

    let lifecycle_state = ProcedureState::Running;

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &lifecycle_state,
            trigger: EventTrigger::Submitted,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();

    assert_eq!(event.event_type(), "create_logical_tables");
    let rows = event.extra_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values,
        vec![
            ValueData::StringValue("greptime".to_string()).into(),
            ValueData::StringValue("public".to_string()).into(),
            ValueData::StringValue("foo".to_string()).into(),
            Value::default(),
            ValueData::U32Value(1024).into(),
        ]
    );
    assert_eq!(
        rows[1].values[2],
        ValueData::StringValue("bar".to_string()).into()
    );
    assert_eq!(rows[1].values[3], Value::default());
    assert_eq!(rows[1].values[4], ValueData::U32Value(1024).into());

    let payload = event.json_payload().unwrap();
    assert_eq!(payload["version"], 1);
    assert_eq!(payload["data"]["kind"], "create_logical_tables");
    assert_eq!(payload["data"]["physical_table_id"], 1024);
    let tables = payload["data"]["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 2);
    assert_eq!(tables[0]["task"], expected_foo_task);
    assert_eq!(tables[1]["task"], expected_bar_task);
    assert!(tables[0]["table_info"].is_object());
}

#[test]
fn test_later_event_is_lightweight() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let procedure = CreateLogicalTablesProcedure::new(
        vec![test_create_logical_table_task("foo")],
        1024,
        ddl_context,
    );
    let lifecycle_state = ProcedureState::Running;

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &lifecycle_state,
            trigger: EventTrigger::Recovered,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();

    assert_eq!(event.event_type(), "create_logical_tables");
    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![Value::default(); 5]
    );
}

#[test]
fn test_succeeded_event_has_one_complete_locator_per_logical_table() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let mut foo = test_create_logical_table_task("foo");
    foo.set_table_id(7);
    let mut bar = test_create_logical_table_task("bar");
    bar.set_table_id(8);
    let procedure = CreateLogicalTablesProcedure::new(vec![foo, bar], 1024, ddl_context);
    let lifecycle_state = ProcedureState::Done {
        output: Some(Arc::new(vec![1025_u32, 1026_u32])),
    };

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &lifecycle_state,
            trigger: EventTrigger::Succeeded,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();

    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    let rows = event.extra_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values,
        vec![
            ValueData::StringValue("greptime".to_string()).into(),
            ValueData::StringValue("public".to_string()).into(),
            ValueData::StringValue("foo".to_string()).into(),
            ValueData::U32Value(1025).into(),
            ValueData::U32Value(1024).into(),
        ]
    );
    assert_eq!(
        rows[1].values[2],
        ValueData::StringValue("bar".to_string()).into()
    );
    assert_eq!(rows[1].values[3], ValueData::U32Value(1026).into());
    assert_eq!(rows[1].values[4], ValueData::U32Value(1024).into());
}

#[test]
fn test_alter_table_submitted_event_has_locator_and_full_versioned_task() {
    let table_id = 1024;
    let task = test_alter_table_task("foo");
    let procedure = AlterTableProcedure::new(
        table_id,
        task.clone(),
        new_ddl_context(Arc::new(MockDatanodeManager::new(()))),
    )
    .unwrap();

    let lifecycle_state = ProcedureState::Running;
    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &lifecycle_state,
            trigger: EventTrigger::Submitted,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();

    assert_eq!(event.event_type(), "alter_table");
    assert_eq!(
        event
            .extra_schema()
            .iter()
            .map(|column| column.column_name.as_str())
            .collect::<Vec<_>>(),
        vec![
            CATALOG_NAME_COLUMN.name(),
            SCHEMA_NAME_COLUMN.name(),
            TABLE_NAME_COLUMN.name(),
            TABLE_ID_COLUMN.name(),
        ]
    );
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![
            api::v1::value::ValueData::StringValue(DEFAULT_CATALOG_NAME.to_string()).into(),
            api::v1::value::ValueData::StringValue(DEFAULT_SCHEMA_NAME.to_string()).into(),
            api::v1::value::ValueData::StringValue("foo".to_string()).into(),
            api::v1::value::ValueData::U32Value(table_id).into(),
        ]
    );
    let payload = event.json_payload().unwrap();
    assert_eq!(payload["version"], TABLE_DDL_PAYLOAD_VERSION);
    let payload_task: AlterTableTask = serde_json::from_value(payload["data"].clone()).unwrap();
    assert_eq!(payload_task, task);
}

#[test]
fn test_alter_table_lifecycle_events_are_lightweight_with_fixed_schema() {
    let procedure = AlterTableProcedure::new(
        1024,
        test_alter_table_task("foo"),
        new_ddl_context(Arc::new(MockDatanodeManager::new(()))),
    )
    .unwrap();
    let lifecycle_state = ProcedureState::Running;
    let procedure_id = ProcedureId::random();
    let submitted = procedure
        .event(&EventContext {
            procedure_id,
            lifecycle_state: &lifecycle_state,
            trigger: EventTrigger::Submitted,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();
    let expected_schema = submitted.extra_schema();
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

    for trigger in triggers {
        let event = procedure
            .event(&EventContext {
                procedure_id,
                lifecycle_state: &lifecycle_state,
                trigger,
                event_type_filter: Arc::new(EventTypeFilter::All),
            })
            .unwrap();

        assert_eq!(event.event_type(), "alter_table");
        assert_eq!(event.extra_schema(), expected_schema);
        assert_eq!(
            event.extra_rows().unwrap()[0].values,
            vec![Value::default(); 4]
        );
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    }
}

#[test]
fn test_submitted_event_has_one_row_per_logical_table_and_full_payload() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let tasks = vec![
        make_alter_logical_table_add_column_task(
            Some(DEFAULT_SCHEMA_NAME),
            "table1",
            vec!["column1".to_string()],
        ),
        make_alter_logical_table_add_column_task(
            Some(DEFAULT_SCHEMA_NAME),
            "table2",
            vec!["column2".to_string()],
        ),
    ];
    let expected_tasks = serde_json::to_value(&tasks).unwrap();
    let physical_table_id = 1024;
    let procedure = AlterLogicalTablesProcedure::new(tasks, physical_table_id, ddl_context);

    let lifecycle_state = ProcedureState::Running;
    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &lifecycle_state,
            trigger: EventTrigger::Submitted,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();

    assert_eq!(event.event_type(), "alter_logical_tables");
    assert_eq!(
        event.json_payload().unwrap(),
        serde_json::json!({
            "version": TABLE_DDL_PAYLOAD_VERSION,
            "data": {
                "kind": "alter_logical_tables",
                "physical_table_id": physical_table_id,
                "tasks": expected_tasks,
            },
        })
    );
    assert_eq!(
        event
            .extra_rows()
            .unwrap()
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        vec![
            vec![
                ValueData::StringValue(DEFAULT_CATALOG_NAME.to_string()).into(),
                ValueData::StringValue(DEFAULT_SCHEMA_NAME.to_string()).into(),
                ValueData::StringValue("table1".to_string()).into(),
                Value::default(),
                ValueData::U32Value(physical_table_id).into(),
            ],
            vec![
                ValueData::StringValue(DEFAULT_CATALOG_NAME.to_string()).into(),
                ValueData::StringValue(DEFAULT_SCHEMA_NAME.to_string()).into(),
                ValueData::StringValue("table2".to_string()).into(),
                Value::default(),
                ValueData::U32Value(physical_table_id).into(),
            ],
        ]
    );
}

#[test]
fn test_later_events_are_lightweight_without_dynamic_success_id() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let procedure = AlterLogicalTablesProcedure::new(
        vec![make_alter_logical_table_add_column_task(
            Some(DEFAULT_SCHEMA_NAME),
            "table1",
            vec!["column1".to_string()],
        )],
        1024,
        ddl_context,
    );
    let lifecycle_state = ProcedureState::Running;
    let child_id = ProcedureId::random();
    let triggers = vec![
        EventTrigger::Recovered,
        EventTrigger::ChildSubmitted {
            procedure_id: child_id,
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

    for trigger in triggers {
        let event = procedure
            .event(&EventContext {
                procedure_id: ProcedureId::random(),
                lifecycle_state: &lifecycle_state,
                trigger,
                event_type_filter: Arc::new(EventTypeFilter::All),
            })
            .unwrap();
        assert_eq!(event.event_type(), "alter_logical_tables");
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
        assert_eq!(
            event.extra_rows().unwrap()[0].values,
            vec![Value::default(); 5]
        );
    }
}

#[test]
fn undrop_submitted_event_uses_id_only_locator_and_payload() {
    let procedure = UndropTableProcedure::new(UndropTableTask { table_id: 42 }, test_context());

    let event = event_for(&procedure, EventTrigger::Submitted);

    assert_eq!(event.event_type(), "undrop_table");
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![
            Value::default(),
            Value::default(),
            Value::default(),
            ValueData::U32Value(42).into(),
        ]
    );
    assert_eq!(
        event.json_payload().unwrap(),
        serde_json::json!({"version": 1, "data": {"table_id": 42}})
    );
}

#[test]
fn purge_submitted_event_contains_only_table_id_without_metadata() {
    let procedure =
        PurgeDroppedTableProcedure::new(PurgeDroppedTableTask { table_id: 43 }, test_context());

    let event = event_for(&procedure, EventTrigger::Submitted);

    assert_eq!(event.event_type(), "purge_dropped_table");
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![
            Value::default(),
            Value::default(),
            Value::default(),
            ValueData::U32Value(43).into(),
        ]
    );
    assert_eq!(
        event.json_payload().unwrap(),
        serde_json::json!({"version": 1, "data": {"table_id": 43}})
    );
}

#[test]
fn truncate_submitted_event_uses_task_identity_and_time_ranges() {
    let time_ranges = vec![(
        Timestamp::new_millisecond(1_000),
        Timestamp::new_millisecond(2_000),
    )];
    let task = TruncateTableTask {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table: "metrics".to_string(),
        table_id: 44,
        time_ranges: time_ranges.clone(),
    };
    let procedure = truncate_procedure(task.clone());

    let event = event_for(&procedure, EventTrigger::Submitted);

    assert_eq!(event.event_type(), "truncate_table");
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![
            ValueData::StringValue(DEFAULT_CATALOG_NAME.to_string()).into(),
            ValueData::StringValue(DEFAULT_SCHEMA_NAME.to_string()).into(),
            ValueData::StringValue("metrics".to_string()).into(),
            ValueData::U32Value(44).into(),
        ]
    );
    let payload = event.json_payload().unwrap();
    assert_eq!(payload["version"], 1);
    assert_eq!(
        serde_json::from_value::<TruncateTableTask>(payload["data"].clone()).unwrap(),
        task
    );
}

#[test]
fn table_procedures_honor_event_type_filter() {
    let create_table =
        CreateTableProcedure::new(test_create_table_task("metrics"), test_context()).unwrap();
    let create_logical_tables = CreateLogicalTablesProcedure::new(
        vec![test_create_logical_table_task("metrics")],
        41,
        test_context(),
    );
    let alter_table =
        AlterTableProcedure::new(42, test_alter_table_task("metrics"), test_context()).unwrap();
    let alter_logical_tables = AlterLogicalTablesProcedure::new(
        vec![make_alter_logical_table_add_column_task(
            Some(DEFAULT_SCHEMA_NAME),
            "metrics",
            vec!["new_tag".to_string()],
        )],
        43,
        test_context(),
    );
    let drop_table = DropTableProcedure::new(
        DropTableTask {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table: "metrics".to_string(),
            table_id: 44,
            drop_if_exists: false,
        },
        test_context(),
    );
    let undrop = UndropTableProcedure::new(UndropTableTask { table_id: 42 }, test_context());
    let purge =
        PurgeDroppedTableProcedure::new(PurgeDroppedTableTask { table_id: 43 }, test_context());
    let truncate = truncate_procedure(TruncateTableTask {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table: "metrics".to_string(),
        table_id: 44,
        time_ranges: vec![],
    });

    for (procedure, event_type) in [
        (&create_table as &dyn Procedure, "create_table"),
        (
            &create_logical_tables as &dyn Procedure,
            "create_logical_tables",
        ),
        (&alter_table as &dyn Procedure, "alter_table"),
        (
            &alter_logical_tables as &dyn Procedure,
            "alter_logical_tables",
        ),
        (&drop_table as &dyn Procedure, "drop_table"),
        (&undrop as &dyn Procedure, "undrop_table"),
        (&purge as &dyn Procedure, "purge_dropped_table"),
        (&truncate as &dyn Procedure, "truncate_table"),
    ] {
        assert_event_filter(procedure, event_type);
    }
}

#[test]
fn later_lifecycle_events_are_lightweight_without_success_ids() {
    let undrop = UndropTableProcedure::new(UndropTableTask { table_id: 42 }, test_context());
    let purge =
        PurgeDroppedTableProcedure::new(PurgeDroppedTableTask { table_id: 43 }, test_context());
    let truncate = truncate_procedure(TruncateTableTask {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table: "metrics".to_string(),
        table_id: 44,
        time_ranges: vec![],
    });
    let procedures: [(&dyn Procedure, &str); 3] = [
        (&undrop, "undrop_table"),
        (&purge, "purge_dropped_table"),
        (&truncate, "truncate_table"),
    ];
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

    for (procedure, expected_type) in procedures {
        for trigger in &triggers {
            let event = event_for(procedure, trigger.clone());

            assert_eq!(event.event_type(), expected_type);
            assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
            assert_eq!(
                event.extra_rows().unwrap()[0].values,
                vec![Value::default(); 4]
            );
        }
    }
}

fn truncate_procedure(task: TruncateTableTask) -> TruncateTableProcedure {
    let table_info = test_create_table_task_with_id("metrics", task.table_id).table_info;
    TruncateTableProcedure::new(
        task,
        DeserializedValueWithBytes::from_inner(TableInfoValue::new(table_info)),
        test_context(),
    )
}

fn test_context() -> crate::ddl::DdlContext {
    new_ddl_context(Arc::new(MockDatanodeManager::new(())))
}

fn assert_event_filter(procedure: &dyn Procedure, event_type: &str) {
    let state = ProcedureState::Running;
    let event_context = |event_type_filter| EventContext {
        procedure_id: ProcedureId::random(),
        lifecycle_state: &state,
        trigger: EventTrigger::Submitted,
        event_type_filter: Arc::new(event_type_filter),
    };

    let allowed = procedure
        .event(&event_context(EventTypeFilter::Only(HashSet::from([
            event_type.to_string(),
        ]))))
        .unwrap();
    assert_eq!(allowed.event_type(), event_type);

    assert!(
        procedure
            .event(&event_context(EventTypeFilter::Only(HashSet::from([
                "other_event".to_string(),
            ]))))
            .is_none()
    );
    assert!(
        procedure
            .event(&event_context(EventTypeFilter::Only(HashSet::new())))
            .is_none()
    );
}

fn event_for(procedure: &dyn Procedure, trigger: EventTrigger) -> Box<dyn Event> {
    let lifecycle_state = ProcedureState::Running;
    procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &lifecycle_state,
            trigger,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap()
}
