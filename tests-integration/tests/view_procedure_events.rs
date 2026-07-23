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
use std::time::Duration;

use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, OutputData};
use common_catalog::consts::DEFAULT_PRIVATE_SCHEMA_NAME;
use common_event_recorder::{
    DEFAULT_EVENTS_TABLE_NAME, EVENTS_TABLE_PAYLOAD_COLUMN_NAME,
    EVENTS_TABLE_TIMESTAMP_COLUMN_NAME, EVENTS_TABLE_TYPE_COLUMN_NAME,
};
use common_procedure::event::{
    EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME, EVENTS_TABLE_PROCEDURE_STATE_COLUMN_NAME,
    EVENTS_TABLE_PROCEDURE_TRIGGER_COLUMN_NAME,
};
use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches};
use common_test_util::temp_dir::create_temp_dir;
use common_wal::config::DatanodeWalConfig;
use datatypes::arrow::array::{Array, AsArray, StringArray, StringViewArray};
use frontend::instance::Instance;
use servers::error::Result as ServerResult;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::test_util::{StorageType, get_test_store_config};

const CREATE_VIEW_EVENT_TYPE: &str = "ddl_create_view";
const DROP_VIEW_EVENT_TYPE: &str = "ddl_drop_view";
const CREATE_VIEW_PROCEDURE_TYPE: &str = "metasrv-procedure::CreateView";
const DROP_VIEW_PROCEDURE_TYPE: &str = "metasrv-procedure::DropView";

const VIEW_NAME: &str = "ddl_view_procedure_events_view";
const SOURCE_TABLE_NAME: &str = "ddl_view_procedure_events_source";

const EVENT_TIMEOUT: Duration = Duration::from_secs(30);
const EVENT_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread")]
async fn test_view_procedure_events() {
    common_telemetry::init_default_ut_logging();

    let store_type = StorageType::File;
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_view_procedure_events_data_home");
    let cluster = GreptimeDbClusterBuilder::new("test_view_procedure_events")
        .await
        .with_shared_home_dir(Arc::new(home_dir))
        .with_datanodes(1)
        .with_store_config(store_config)
        .with_datanode_wal_config(DatanodeWalConfig::Noop)
        .build(true)
        .await;
    let instance = cluster.fe_instance().clone();

    run_sql(
        &instance,
        &format!(
            "CREATE TABLE {SOURCE_TABLE_NAME} (host STRING PRIMARY KEY, amount DOUBLE, ts TIMESTAMP TIME INDEX)"
        ),
        QueryContext::arc(),
    )
    .await
    .unwrap();

    let before_create = procedure_snapshot(&instance, CREATE_VIEW_PROCEDURE_TYPE).await;
    run_sql(
        &instance,
        &format!("CREATE VIEW {VIEW_NAME} AS SELECT amount FROM {SOURCE_TABLE_NAME}"),
        QueryContext::arc(),
    )
    .await
    .unwrap();
    let after_create = procedure_snapshot(&instance, CREATE_VIEW_PROCEDURE_TYPE).await;
    let create_procedure_id =
        new_completed_procedure_id(&before_create, after_create, CREATE_VIEW_PROCEDURE_TYPE);

    let create_events =
        wait_for_view_events(&instance, CREATE_VIEW_EVENT_TYPE, &create_procedure_id).await;
    let create_succeeded = event_for_trigger(&create_events, "Succeeded");
    let view_id = create_succeeded
        .view_id
        .expect("create event should contain view_id");
    assert_create_view_events(&create_events, view_id);

    let before_drop = procedure_snapshot(&instance, DROP_VIEW_PROCEDURE_TYPE).await;
    run_sql(
        &instance,
        &format!("DROP VIEW {VIEW_NAME}"),
        QueryContext::arc(),
    )
    .await
    .unwrap();
    let after_drop = procedure_snapshot(&instance, DROP_VIEW_PROCEDURE_TYPE).await;
    let drop_procedure_id =
        new_completed_procedure_id(&before_drop, after_drop, DROP_VIEW_PROCEDURE_TYPE);

    let drop_events =
        wait_for_view_events(&instance, DROP_VIEW_EVENT_TYPE, &drop_procedure_id).await;
    assert_drop_view_events(&drop_events, view_id);
}

#[derive(Debug)]
struct ProcedureSnapshot {
    procedure_id: String,
    procedure_type: String,
    status: String,
}

async fn procedure_snapshot(
    instance: &Arc<Instance>,
    procedure_type: &str,
) -> Vec<ProcedureSnapshot> {
    let query = format!(
        "SELECT procedure_id, procedure_type, status \
         FROM information_schema.procedure_info \
         WHERE procedure_type = '{procedure_type}'"
    );
    let output = run_sql(instance, &query, QueryContext::arc())
        .await
        .unwrap();
    let OutputData::Stream(stream) = output.data else {
        panic!("procedure_info query did not return a stream");
    };
    let batches = RecordBatches::try_collect(stream).await.unwrap();

    let mut snapshot = Vec::new();
    for batch in batches.iter() {
        let procedure_ids = batch
            .column_by_name("procedure_id")
            .unwrap()
            .as_string::<i32>();
        let procedure_types = batch
            .column_by_name("procedure_type")
            .unwrap()
            .as_string::<i32>();
        let statuses = batch.column_by_name("status").unwrap().as_string::<i32>();

        for row in 0..batch.num_rows() {
            snapshot.push(ProcedureSnapshot {
                procedure_id: procedure_ids.value(row).to_string(),
                procedure_type: procedure_types.value(row).to_string(),
                status: statuses.value(row).to_string(),
            });
        }
    }

    snapshot
}

fn new_completed_procedure_id(
    before: &[ProcedureSnapshot],
    after: Vec<ProcedureSnapshot>,
    procedure_type: &str,
) -> String {
    let before_ids: HashSet<_> = before
        .iter()
        .map(|procedure| procedure.procedure_id.as_str())
        .collect();
    let new_procedures: Vec<_> = after
        .into_iter()
        .filter(|procedure| {
            !before_ids.contains(procedure.procedure_id.as_str())
                && procedure.status == "Done"
                && procedure.procedure_type == procedure_type
        })
        .collect();

    assert_eq!(
        new_procedures.len(),
        1,
        "expected one newly completed view procedure, found {new_procedures:?}"
    );
    new_procedures[0].procedure_id.clone()
}

#[derive(Debug)]
struct ViewEvent {
    state: String,
    trigger: String,
    catalog_name: Option<String>,
    schema_name: Option<String>,
    view_name: Option<String>,
    view_id: Option<u32>,
    payload: serde_json::Value,
}

async fn wait_for_view_events(
    instance: &Arc<Instance>,
    event_type: &str,
    procedure_id: &str,
) -> Vec<ViewEvent> {
    let mut last_error = None;
    let result = tokio::time::timeout(EVENT_TIMEOUT, async {
        loop {
            match query_view_events(instance, event_type, procedure_id).await {
                Ok(events) => {
                    let has_submitted = events.iter().any(|event| event.trigger == "Submitted");
                    let has_succeeded = events.iter().any(|event| event.trigger == "Succeeded");
                    if has_submitted && has_succeeded {
                        assert_event_lifecycle(&events);
                        return events;
                    }
                }
                Err(error) => last_error = Some(error),
            }
            tokio::time::sleep(EVENT_POLL_INTERVAL).await;
        }
    })
    .await;

    result.unwrap_or_else(|_| {
        let last_error = last_error.unwrap_or_else(|| "none".to_string());
        panic!("timed out waiting for {event_type} events for procedure {procedure_id}; last query error: {last_error}")
    })
}

async fn query_view_events(
    instance: &Arc<Instance>,
    event_type: &str,
    procedure_id: &str,
) -> Result<Vec<ViewEvent>, String> {
    let query = format!(
        "SELECT {EVENTS_TABLE_PROCEDURE_STATE_COLUMN_NAME}, \
                {EVENTS_TABLE_PROCEDURE_TRIGGER_COLUMN_NAME}, \
                catalog_name, schema_name, view_name, view_id, \
                json_to_string({EVENTS_TABLE_PAYLOAD_COLUMN_NAME}) AS payload \
         FROM {DEFAULT_PRIVATE_SCHEMA_NAME}.{DEFAULT_EVENTS_TABLE_NAME} \
         WHERE {EVENTS_TABLE_TYPE_COLUMN_NAME} = '{event_type}' \
           AND {EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME} = '{procedure_id}' \
         ORDER BY {EVENTS_TABLE_TIMESTAMP_COLUMN_NAME} ASC"
    );
    let output = run_sql(instance, &query, QueryContext::arc())
        .await
        .map_err(|error| format!("query failed: {error}"))?;
    let OutputData::Stream(stream) = output.data else {
        return Err("schema error: event query did not return a stream".to_string());
    };
    let batches = RecordBatches::try_collect(stream)
        .await
        .map_err(|error| format!("collection failed: {error}"))?;

    let mut events = Vec::new();
    for batch in batches.iter() {
        let states = string_column(batch, EVENTS_TABLE_PROCEDURE_STATE_COLUMN_NAME)?;
        let triggers = string_column(batch, EVENTS_TABLE_PROCEDURE_TRIGGER_COLUMN_NAME)?;
        let catalog_names = string_column(batch, "catalog_name")?;
        let schema_names = string_column(batch, "schema_name")?;
        let view_names = string_column(batch, "view_name")?;
        let view_ids = required_column(batch, "view_id")?
            .as_any()
            .downcast_ref::<datatypes::arrow::array::UInt32Array>()
            .ok_or_else(|| schema_error(batch, "view_id", "UInt32"))?;
        let payloads = required_column(batch, "payload")?
            .as_any()
            .downcast_ref::<StringViewArray>()
            .ok_or_else(|| schema_error(batch, "payload", "Utf8View"))?;

        for row in 0..batch.num_rows() {
            events.push(ViewEvent {
                state: states.value(row).to_string(),
                trigger: triggers.value(row).to_string(),
                catalog_name: nullable_string(catalog_names, row),
                schema_name: nullable_string(schema_names, row),
                view_name: nullable_string(view_names, row),
                view_id: (!view_ids.is_null(row)).then(|| view_ids.value(row)),
                payload: serde_json::from_str(payloads.value(row)).map_err(|error| {
                    format!("JSON decode failed for payload at row {row}: {error}")
                })?,
            });
        }
    }

    Ok(events)
}

fn required_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a dyn Array, String> {
    batch
        .column_by_name(name)
        .map(|column| column.as_ref())
        .ok_or_else(|| schema_error(batch, name, "present"))
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, String> {
    required_column(batch, name)?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| schema_error(batch, name, "Utf8"))
}

fn schema_error(batch: &RecordBatch, name: &str, expected: &str) -> String {
    let actual = batch
        .column_by_name(name)
        .map(|column| format!("{:?}", column.data_type()))
        .unwrap_or_else(|| "missing".to_string());
    format!("schema error for column {name}: expected {expected}, got {actual}")
}

fn assert_event_lifecycle(events: &[ViewEvent]) {
    assert_eq!(
        events.len(),
        2,
        "expected exactly two lifecycle events, found {events:?}"
    );
    assert_eq!(events[0].trigger, "Submitted");
    assert_eq!(events[0].state, "Running");
    assert_eq!(events[1].trigger, "Succeeded");
    assert_eq!(events[1].state, "Done");
}

fn nullable_string(array: &datatypes::arrow::array::StringArray, row: usize) -> Option<String> {
    (!array.is_null(row)).then(|| array.value(row).to_string())
}

fn event_for_trigger<'a>(events: &'a [ViewEvent], trigger: &str) -> &'a ViewEvent {
    events
        .iter()
        .find(|event| event.trigger == trigger)
        .unwrap_or_else(|| panic!("missing {trigger} event in {events:?}"))
}

fn assert_create_view_events(events: &[ViewEvent], view_id: u32) {
    let submitted = event_for_trigger(events, "Submitted");
    assert_eq!(submitted.state, "Running");
    assert_eq!(
        submitted.catalog_name.as_deref(),
        Some(DEFAULT_CATALOG_NAME)
    );
    assert_eq!(submitted.schema_name.as_deref(), Some(DEFAULT_SCHEMA_NAME));
    assert_eq!(submitted.view_name.as_deref(), Some(VIEW_NAME));
    assert_eq!(submitted.view_id, None);
    assert_eq!(
        submitted.payload,
        serde_json::json!({
            "version": 1,
            "or_replace": false,
            "create_if_not_exists": false,
            "referenced_table_count": 1,
            "column_count": 0,
        })
    );

    let succeeded = event_for_trigger(events, "Succeeded");
    assert_eq!(succeeded.state, "Done");
    assert_eq!(succeeded.catalog_name, None);
    assert_eq!(succeeded.schema_name, None);
    assert_eq!(succeeded.view_name, None);
    assert_eq!(succeeded.view_id, Some(view_id));
    assert_eq!(succeeded.payload, serde_json::Value::Null);
}

fn assert_drop_view_events(events: &[ViewEvent], view_id: u32) {
    let submitted = event_for_trigger(events, "Submitted");
    assert_eq!(submitted.state, "Running");
    assert_eq!(
        submitted.catalog_name.as_deref(),
        Some(DEFAULT_CATALOG_NAME)
    );
    assert_eq!(submitted.schema_name.as_deref(), Some(DEFAULT_SCHEMA_NAME));
    assert_eq!(submitted.view_name.as_deref(), Some(VIEW_NAME));
    assert_eq!(submitted.view_id, Some(view_id));
    assert_eq!(
        submitted.payload,
        serde_json::json!({
            "version": 1,
            "drop_if_exists": false,
        })
    );

    let succeeded = event_for_trigger(events, "Succeeded");
    assert_eq!(succeeded.state, "Done");
    assert_eq!(succeeded.catalog_name, None);
    assert_eq!(succeeded.schema_name, None);
    assert_eq!(succeeded.view_name, None);
    assert_eq!(succeeded.view_id, None);
    assert_eq!(succeeded.payload, serde_json::Value::Null);
}

async fn run_sql(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> ServerResult<Output> {
    instance.do_query(sql, query_ctx).await.remove(0)
}
