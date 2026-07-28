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
use std::time::Duration;

use client::OutputData;
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN, PAYLOAD_COLUMN, PHYSICAL_TABLE_ID_COLUMN, PROCEDURE_ID_COLUMN,
    PROCEDURE_TRIGGER_COLUMN, SCHEMA_NAME_COLUMN, TABLE_ID_COLUMN, TABLE_NAME_COLUMN, TYPE_COLUMN,
};
use common_test_util::temp_dir::create_temp_dir;
use frontend::instance::Instance;
use meta_srv::gc::GcSchedulerOptions;
use mito2::gc::GcConfig;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::cluster::GreptimeDbClusterBuilder;

use crate::event_recorder_test_util::{
    assert_eventually_eq, assert_single_event, find_eventually_string,
};

const EVENTS_TABLE: &str = "greptime_private.events";
const TABLE: &str = "table_ddl_events";
const PHYSICAL_TABLE: &str = "table_ddl_events_phy";
const LOGICAL_TABLE: &str = "table_ddl_events_logical";

#[tokio::test(flavor = "multi_thread")]
async fn test_table_ddl_procedure_events() {
    common_telemetry::init_default_ut_logging();

    // Arrange: use a distributed cluster to submit Undrop and Purge directly
    // through the metasrv DDL manager and exercise logical-table task grouping.
    let home_dir = create_temp_dir("table_ddl_procedure_events");
    let mut gc_options = GcSchedulerOptions {
        enable: true,
        ..Default::default()
    };
    gc_options.experimental_soft_drop.enable = true;
    gc_options.experimental_soft_drop.retention = Duration::from_secs(1);
    let cluster = GreptimeDbClusterBuilder::new("table_ddl_procedure_events")
        .await
        .with_datanodes(1)
        .with_metasrv_gc_config(gc_options)
        .with_datanode_gc_config(GcConfig {
            enable: true,
            ..Default::default()
        })
        .with_shared_home_dir(Arc::new(home_dir))
        .build(true)
        .await;
    let frontend = cluster.fe_instance().clone();

    // Act / Assert: Create Table retains its rich submitted row.
    run_sql(
        &frontend,
        &format!(
            "CREATE TABLE {TABLE} (host STRING PRIMARY KEY, ts TIMESTAMP TIME INDEX, val DOUBLE)"
        ),
    )
    .await;
    assert_named_submitted_event(
        &frontend,
        "create_table",
        "\
+--------------+------------------+
| type         | name             |
+--------------+------------------+
| create_table | table_ddl_events |
+--------------+------------------+",
    )
    .await;
    assert_id_terminal_event(
        &frontend,
        "create_table",
        "Succeeded",
        "\
+--------------+------+
| type         | id   |
+--------------+------+
| create_table | 1024 |
+--------------+------+",
    )
    .await;

    // Act / Assert: Alter and Truncate have a rich submitted row and lightweight
    // terminal lifecycle row.
    run_sql(
        &frontend,
        &format!("ALTER TABLE {TABLE} ADD COLUMN extra STRING"),
    )
    .await;
    assert_named_id_submitted_event(
        &frontend,
        "alter_table",
        "\
+-------------+------------------+------+
| type        | name             | id   |
+-------------+------------------+------+
| alter_table | table_ddl_events | 1024 |
+-------------+------------------+------+",
    )
    .await;
    assert_lightweight_terminal_event(&frontend, "alter_table", "Succeeded").await;
    run_sql(
        &frontend,
        &format!("INSERT INTO {TABLE} VALUES ('a', 0, 1, 'b')"),
    )
    .await;
    run_sql(&frontend, &format!("TRUNCATE TABLE {TABLE}")).await;
    assert_named_id_submitted_event(
        &frontend,
        "truncate_table",
        "\
+----------------+------------------+------+
| type           | name             | id   |
+----------------+------------------+------+
| truncate_table | table_ddl_events | 1024 |
+----------------+------------------+------+",
    )
    .await;
    assert_lightweight_terminal_event(&frontend, "truncate_table", "Succeeded").await;

    // Act / Assert: Drop records the table name, while Undrop and Purge use the
    // deterministic table ID as their submitted locator.
    run_sql(&frontend, &format!("DROP TABLE {TABLE}")).await;
    assert_named_id_submitted_event(
        &frontend,
        "drop_table",
        "\
+------------+------------------+------+
| type       | name             | id   |
+------------+------------------+------+
| drop_table | table_ddl_events | 1024 |
+------------+------------------+------+",
    )
    .await;
    assert_lightweight_terminal_event(&frontend, "drop_table", "Succeeded").await;
    cluster
        .metasrv
        .ddl_manager()
        .submit_undrop_table_task(common_meta::rpc::ddl::UndropTableTask { table_id: 1024 })
        .await
        .unwrap();
    assert_id_submitted_event(
        &frontend,
        "undrop_table",
        "\
+--------------+------+
| type         | id   |
+--------------+------+
| undrop_table | 1024 |
+--------------+------+",
    )
    .await;
    assert_lightweight_terminal_event(&frontend, "undrop_table", "Succeeded").await;
    run_sql(&frontend, &format!("DROP TABLE {TABLE}")).await;
    cluster
        .metasrv
        .ddl_manager()
        .submit_purge_dropped_table_task(common_meta::rpc::ddl::PurgeDroppedTableTask {
            table_id: 1024,
        })
        .await
        .unwrap();
    assert_id_submitted_event(
        &frontend,
        "purge_dropped_table",
        "\
+---------------------+------+
| type                | id   |
+---------------------+------+
| purge_dropped_table | 1024 |
+---------------------+------+",
    )
    .await;
    assert_lightweight_terminal_event(&frontend, "purge_dropped_table", "Succeeded").await;

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

    // Assert: logical Create emits one submitted row per logical table.
    assert_named_submitted_event(
        &frontend,
        "create_logical_tables",
        "\
+-----------------------+--------------------------+
| type                  | name                     |
+-----------------------+--------------------------+
| create_logical_tables | table_ddl_events_logical |
+-----------------------+--------------------------+",
    )
    .await;
    assert_logical_terminal_event(
        &frontend,
        "create_logical_tables",
        "Succeeded",
        "\
+-----------------------+--------------------------+------+-------------+
| type                  | name                     | id   | physical_id |
+-----------------------+--------------------------+------+-------------+
| create_logical_tables | table_ddl_events_logical | 1027 | 1026        |
+-----------------------+--------------------------+------+-------------+",
    )
    .await;

    // Act / Assert: logical Alter preserves the one-row-per-logical-table submitted
    // contract and emits a lightweight terminal row.
    run_sql(
        &frontend,
        &format!("ALTER TABLE {LOGICAL_TABLE} ADD COLUMN rack STRING PRIMARY KEY"),
    )
    .await;
    assert_named_submitted_event(
        &frontend,
        "alter_logical_tables",
        "\
+----------------------+--------------------------+
| type                 | name                     |
+----------------------+--------------------------+
| alter_logical_tables | table_ddl_events_logical |
+----------------------+--------------------------+",
    )
    .await;
    assert_lightweight_terminal_event(&frontend, "alter_logical_tables", "Succeeded").await;
}

async fn run_sql(instance: &Arc<Instance>, sql: &str) {
    let output = instance
        .do_query(sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    assert!(matches!(output.data, OutputData::AffectedRows(_)), "{sql}");
}

async fn assert_named_submitted_event(instance: &Arc<Instance>, event_type: &str, expected: &str) {
    let query = format!(
        "SELECT {}, {} AS name FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = 'Submitted' AND json_path_match({}, '$.version == 1') ORDER BY timestamp DESC LIMIT 1",
        TYPE_COLUMN.name(),
        TABLE_NAME_COLUMN.name(),
        TYPE_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
    );
    assert_eventually_eq(instance, &query, expected).await;
}

async fn assert_named_id_submitted_event(
    instance: &Arc<Instance>,
    event_type: &str,
    expected: &str,
) {
    let query = format!(
        "SELECT {}, {} AS name, {} AS id FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = 'Submitted' AND json_path_match({}, '$.version == 1') ORDER BY timestamp DESC LIMIT 1",
        TYPE_COLUMN.name(),
        TABLE_NAME_COLUMN.name(),
        TABLE_ID_COLUMN.name(),
        TYPE_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
    );
    assert_eventually_eq(instance, &query, expected).await;
}

async fn assert_id_submitted_event(instance: &Arc<Instance>, event_type: &str, expected: &str) {
    let query = format!(
        "SELECT {}, {} AS id FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = 'Submitted' AND json_path_match({}, '$.version == 1') ORDER BY timestamp DESC LIMIT 1",
        TYPE_COLUMN.name(),
        TABLE_ID_COLUMN.name(),
        TYPE_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
    );
    assert_eventually_eq(instance, &query, expected).await;
}

async fn submitted_procedure_id(instance: &Arc<Instance>, event_type: &str) -> String {
    let query = format!(
        "SELECT {} FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = 'Submitted' AND json_path_match({}, '$.version == 1') ORDER BY timestamp DESC LIMIT 1",
        PROCEDURE_ID_COLUMN.name(),
        TYPE_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
    );
    find_eventually_string(instance, &query, PROCEDURE_ID_COLUMN.name()).await
}

async fn assert_id_terminal_event(
    instance: &Arc<Instance>,
    event_type: &str,
    terminal_trigger: &str,
    expected: &str,
) {
    let procedure_id = submitted_procedure_id(instance, event_type).await;
    let query = format!(
        "SELECT {}, {} AS id FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = '{procedure_id}' AND {} = '{terminal_trigger}' AND json_is_null({}) AND {} IS NULL AND {} IS NULL AND {} IS NULL",
        TYPE_COLUMN.name(),
        TABLE_ID_COLUMN.name(),
        TYPE_COLUMN.name(),
        PROCEDURE_ID_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
        CATALOG_NAME_COLUMN.name(),
        SCHEMA_NAME_COLUMN.name(),
        TABLE_NAME_COLUMN.name(),
    );
    assert_eventually_eq(instance, &query, expected).await;
}

async fn assert_logical_terminal_event(
    instance: &Arc<Instance>,
    event_type: &str,
    terminal_trigger: &str,
    expected: &str,
) {
    let procedure_id = submitted_procedure_id(instance, event_type).await;
    let query = format!(
        "SELECT {}, {} AS name, {} AS id, {} AS physical_id FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = '{procedure_id}' AND {} = '{terminal_trigger}' AND json_is_null({})",
        TYPE_COLUMN.name(),
        TABLE_NAME_COLUMN.name(),
        TABLE_ID_COLUMN.name(),
        PHYSICAL_TABLE_ID_COLUMN.name(),
        TYPE_COLUMN.name(),
        PROCEDURE_ID_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
    );
    assert_eventually_eq(instance, &query, expected).await;
}

async fn assert_lightweight_terminal_event(
    instance: &Arc<Instance>,
    event_type: &str,
    terminal_trigger: &str,
) {
    let procedure_id = submitted_procedure_id(instance, event_type).await;
    let query = format!(
        "SELECT count(*) AS event_count FROM {EVENTS_TABLE} WHERE {} = '{event_type}' AND {} = '{procedure_id}' AND {} = '{terminal_trigger}' AND json_is_null({}) AND {} IS NULL AND {} IS NULL AND {} IS NULL AND {} IS NULL",
        TYPE_COLUMN.name(),
        PROCEDURE_ID_COLUMN.name(),
        PROCEDURE_TRIGGER_COLUMN.name(),
        PAYLOAD_COLUMN.name(),
        CATALOG_NAME_COLUMN.name(),
        SCHEMA_NAME_COLUMN.name(),
        TABLE_NAME_COLUMN.name(),
        TABLE_ID_COLUMN.name(),
    );
    assert_single_event(instance, &query).await;
}
