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

use std::sync::Arc;

use api::v1::Value;
use api::v1::value::ValueData;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_event_recorder::Event;
use common_procedure::{
    ChildSubmissionOutcome, EventContext, EventTrigger, Procedure, ProcedureId, ProcedureState,
    RetryPhase,
};
use common_time::Timestamp;

use crate::ddl::purge_dropped_table::PurgeDroppedTableProcedure;
use crate::ddl::test_util::create_table::test_create_table_task;
use crate::ddl::truncate_table::TruncateTableProcedure;
use crate::ddl::undrop_table::UndropTableProcedure;
use crate::key::DeserializedValueWithBytes;
use crate::key::table_info::TableInfoValue;
use crate::rpc::ddl::{PurgeDroppedTableTask, TruncateTableTask, UndropTableTask};
use crate::test_util::{MockDatanodeManager, new_ddl_context};

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
    let table_info = test_create_table_task("metrics", task.table_id).table_info;
    TruncateTableProcedure::new(
        task,
        DeserializedValueWithBytes::from_inner(TableInfoValue::new(table_info)),
        test_context(),
    )
}

fn test_context() -> crate::ddl::DdlContext {
    new_ddl_context(Arc::new(MockDatanodeManager::new(())))
}

fn event_for(procedure: &dyn Procedure, trigger: EventTrigger) -> Box<dyn Event> {
    let lifecycle_state = ProcedureState::Running;
    procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &lifecycle_state,
            trigger,
        })
        .unwrap()
}
