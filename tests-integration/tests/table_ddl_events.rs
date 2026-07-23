// Copyright 2026 Greptime Team
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
use std::time::Duration;

use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, OutputData};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_event_recorder::EVENTS_TABLE_PAYLOAD_COLUMN_NAME;
use common_procedure::event::{
    EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME, EVENTS_TABLE_PROCEDURE_STATE_COLUMN_NAME,
    EVENTS_TABLE_PROCEDURE_TRIGGER_COLUMN_NAME,
};
use common_recordbatch::RecordBatches;
use common_test_util::temp_dir::create_temp_dir;
use datatypes::arrow::array::AsArray;
use frontend::instance::Instance;
use servers::error::Result as ServerResult;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::cluster::GreptimeDbClusterBuilder;

const EVENTS_TABLE: &str = "greptime_private.events";
const TABLE: &str = "table_ddl_events";
const PHYSICAL_TABLE: &str = "table_ddl_events_phy";
const LOGICAL_TABLE: &str = "table_ddl_events_logical";

#[tokio::test(flavor = "multi_thread")]
async fn test_table_ddl_procedure_events() {
    common_telemetry::init_default_ut_logging();

    // Arrange: use a distributed cluster. Standalone does not wire an EventRecorder
    // into its procedure manager.
    let home_dir = create_temp_dir("table_ddl_procedure_events");
    let cluster = GreptimeDbClusterBuilder::new("table_ddl_procedure_events")
        .await
        .with_datanodes(1)
        .with_ddl_soft_drop_enabled(true)
        .with_shared_home_dir(Arc::new(home_dir))
        .build(true)
        .await;
    let frontend = cluster.fe_instance().clone();

    // Act / Assert: Create Table retains its rich submitted row, while success only
    // contains the dynamically allocated table ID.
    run_sql(
        &frontend,
        &format!(
            "CREATE TABLE {TABLE} (host STRING PRIMARY KEY, ts TIMESTAMP TIME INDEX, val DOUBLE)"
        ),
    )
    .await;
    let created_table_id = table_id(&frontend, TABLE).await;
    assert_event_contract(
        &frontend,
        "create_table",
        TABLE,
        "Succeeded",
        true,
        Some(created_table_id),
    )
    .await;

    // Act / Assert: Alter and Truncate have a rich submitted row and lightweight
    // terminal lifecycle row.
    run_sql(
        &frontend,
        &format!("ALTER TABLE {TABLE} ADD COLUMN extra STRING"),
    )
    .await;
    assert_event_contract(&frontend, "alter_table", TABLE, "Succeeded", false, None).await;
    run_sql(
        &frontend,
        &format!("INSERT INTO {TABLE} VALUES ('a', 0, 1, 'b')"),
    )
    .await;
    run_sql(&frontend, &format!("TRUNCATE TABLE {TABLE}")).await;
    assert_event_contract(&frontend, "truncate_table", TABLE, "Succeeded", false, None).await;

    // Act / Assert: Drop, Undrop, and Purge all record the submitted locator/payload
    // and a lightweight subsequent row. The same allocated ID is recovered from the
    // catalog rather than assumed by the test.
    run_sql(&frontend, &format!("DROP TABLE {TABLE}")).await;
    assert_event_contract(&frontend, "drop_table", TABLE, "Succeeded", false, None).await;
    cluster
        .metasrv
        .ddl_manager()
        .submit_undrop_table_task(common_meta::rpc::ddl::UndropTableTask {
            table_id: created_table_id,
        })
        .await
        .unwrap();
    assert_event_contract(
        &frontend,
        "undrop_table",
        "",
        "Succeeded",
        false,
        Some(created_table_id),
    )
    .await;
    run_sql(&frontend, &format!("DROP TABLE {TABLE}")).await;
    cluster
        .metasrv
        .ddl_manager()
        .submit_purge_dropped_table_task(common_meta::rpc::ddl::PurgeDroppedTableTask {
            table_id: created_table_id,
        })
        .await
        .unwrap();
    assert_event_contract(
        &frontend,
        "purge_dropped_table",
        "",
        "Succeeded",
        false,
        Some(created_table_id),
    )
    .await;

    // Act / Assert: after a tombstone is purged, Undrop fails in the procedure and
    // records the failure lifecycle event. This intentionally depends on the
    // parallel semantic fix that moves the missing-tombstone check into the procedure.
    assert!(
        cluster
            .metasrv
            .ddl_manager()
            .submit_undrop_table_task(common_meta::rpc::ddl::UndropTableTask {
                table_id: created_table_id,
            })
            .await
            .is_err()
    );
    assert_event_contract(
        &frontend,
        "undrop_table",
        "",
        "Failed",
        false,
        Some(created_table_id),
    )
    .await;

    // Arrange / Act: metric logical-table DDL is executed through the distributed
    // frontend, which groups logical DDL tasks for the meta procedure manager.
    run_sql(
        &frontend,
        &format!(
            "CREATE TABLE {PHYSICAL_TABLE} (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH (\"physical_metric_table\" = \"\")"
        ),
    )
    .await;
    run_sql(
        &frontend,
        &format!(
            "CREATE TABLE {LOGICAL_TABLE} (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) ENGINE=metric WITH (\"on_physical_table\" = \"{PHYSICAL_TABLE}\")"
        ),
    )
    .await;
    let logical_table_id = table_id(&frontend, LOGICAL_TABLE).await;

    // Assert: logical Create emits one submitted row per logical table and its
    // success row carries only the allocated logical table ID.
    assert_event_contract(
        &frontend,
        "create_logical_tables",
        LOGICAL_TABLE,
        "Succeeded",
        true,
        Some(logical_table_id),
    )
    .await;

    // Act / Assert: logical Alter preserves the one-row-per-logical-table submitted
    // contract and emits a lightweight terminal row.
    run_sql(
        &frontend,
        &format!("ALTER TABLE {LOGICAL_TABLE} ADD COLUMN rack STRING PRIMARY KEY"),
    )
    .await;
    assert_event_contract(
        &frontend,
        "alter_logical_tables",
        LOGICAL_TABLE,
        "Succeeded",
        false,
        None,
    )
    .await;
}

async fn run_sql(instance: &Arc<Instance>, sql: &str) {
    let output = instance
        .do_query(sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    assert!(matches!(output.data, OutputData::AffectedRows(_)), "{sql}");
}

async fn table_id(instance: &Arc<Instance>, table: &str) -> u32 {
    let table = instance
        .catalog_manager()
        .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table, None)
        .await
        .unwrap()
        .unwrap();
    table.table_info().table_id()
}

/// Polls the event table instead of sleeping for a flush interval. Event recording is
/// deliberately asynchronous and best-effort, so a fixed delay makes this integration
/// test unnecessarily flaky on loaded CI workers.
async fn assert_event_contract(
    instance: &Arc<Instance>,
    event_type: &str,
    submitted_table_name: &str,
    terminal_state: &str,
    id_only_success: bool,
    table_id: Option<u32>,
) {
    let expected_table = submitted_table_name.to_string();
    let mut last = format!("waiting for a {event_type} event in {EVENTS_TABLE}");
    let mut last_query_error = None;
    for _ in 0..60 {
        let table_predicate = if expected_table.is_empty() {
            String::new()
        } else {
            format!(" AND table_name = '{expected_table}'")
        };
        let id_predicate = table_id
            .map(|id| format!(" AND (table_id = {id} OR table_id IS NULL)"))
            .unwrap_or_default();
        let submitted_sql = format!(
            "SELECT {EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME} FROM {EVENTS_TABLE} WHERE type = '{event_type}' AND {EVENTS_TABLE_PROCEDURE_TRIGGER_COLUMN_NAME} = 'Submitted' AND json_path_match({EVENTS_TABLE_PAYLOAD_COLUMN_NAME}, '$.version == 1'){table_predicate}{id_predicate} ORDER BY timestamp DESC LIMIT 1"
        );
        let procedure_id = match query_procedure_id(instance, &submitted_sql).await {
            Ok(Some(procedure_id)) => procedure_id,
            Ok(None) => {
                last = format!("no submitted {event_type} event found in {EVENTS_TABLE}");
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            Err(error) if error.status_code() == StatusCode::TableNotFound => {
                last = format!("{EVENTS_TABLE} is not available before the recorder's first flush");
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            Err(error) => {
                last_query_error = Some(error.to_string());
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
        };
        let table_id_predicate = if id_only_success {
            format!(
                " AND table_id = {}",
                table_id.expect("table ID is required")
            )
        } else {
            " AND table_id IS NULL".to_string()
        };
        let terminal_sql = format!(
            "SELECT {EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME} FROM {EVENTS_TABLE} WHERE type = '{event_type}' AND {EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME} = '{procedure_id}' AND {EVENTS_TABLE_PROCEDURE_TRIGGER_COLUMN_NAME} = '{terminal_state}' AND json_is_null({EVENTS_TABLE_PAYLOAD_COLUMN_NAME}) AND catalog_name IS NULL AND schema_name IS NULL AND table_name IS NULL{table_id_predicate} LIMIT 1"
        );
        match query_procedure_id(instance, &terminal_sql).await {
            Ok(Some(_)) => return,
            Ok(None) => {
                let sql = format!(
                    "SELECT {EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME}, {EVENTS_TABLE_PROCEDURE_TRIGGER_COLUMN_NAME}, {EVENTS_TABLE_PROCEDURE_STATE_COLUMN_NAME}, catalog_name, schema_name, table_name, table_id, {EVENTS_TABLE_PAYLOAD_COLUMN_NAME} FROM {EVENTS_TABLE} WHERE type = '{event_type}' AND {EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME} = '{procedure_id}' ORDER BY timestamp"
                );
                last = query_pretty(instance, &sql).await;
            }
            Err(error) => last_query_error = Some(error.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let last_query_error = last_query_error
        .map(|error| format!("; last query error: {error}"))
        .unwrap_or_default();
    panic!("timed out waiting for {event_type} event contract: {last}{last_query_error}");
}

async fn query_pretty(instance: &Arc<Instance>, sql: &str) -> String {
    let output = instance
        .do_query(sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    let OutputData::Stream(stream) = output.data else {
        unreachable!("event query must return a stream");
    };
    RecordBatches::try_collect(stream)
        .await
        .unwrap()
        .pretty_print()
        .unwrap()
}

async fn query_procedure_id(instance: &Arc<Instance>, sql: &str) -> ServerResult<Option<String>> {
    let output = instance
        .do_query(sql, QueryContext::arc())
        .await
        .remove(0)?;
    let OutputData::Stream(stream) = output.data else {
        unreachable!("procedure-id query must return a stream");
    };
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let Some(batch) = batches.take().into_iter().next() else {
        return Ok(None);
    };
    let Some(column) = batch.column_by_name(EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME) else {
        return Ok(None);
    };
    let column = column.as_string::<i32>();
    Ok(column.iter().next().flatten().map(ToString::to_string))
}
