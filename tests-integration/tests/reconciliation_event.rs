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

use client::OutputData;
use common_meta::reconciliation::ResolveStrategy;
use common_procedure::watcher;
use frontend::instance::Instance;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use table::table_reference::TableReference;
use tests_integration::cluster::GreptimeDbClusterBuilder;

use crate::event_recorder_test_util::{assert_eventually_eq, find_eventually_string};

const EVENTS_TABLE: &str = "greptime_private.events";
const CATALOG: &str = "greptime";
const DATABASE: &str = "reconciliation_event_database";

#[tokio::test(flavor = "multi_thread")]
async fn test_catalog_and_database_reconciliation_events() {
    common_telemetry::init_default_ut_logging();

    let cluster = GreptimeDbClusterBuilder::new("catalog_database_reconciliation_events")
        .await
        .with_datanodes(1)
        .build(true)
        .await;
    let frontend = cluster.fe_instance().clone();
    run_sql(frontend.as_ref(), &format!("CREATE DATABASE {DATABASE}")).await;
    run_sql(
        frontend.as_ref(),
        &format!("CREATE TABLE {DATABASE}.metrics (ts TIMESTAMP TIME INDEX)"),
    )
    .await;

    let catalog_procedure_id = cluster
        .metasrv
        .reconciliation_manager()
        .reconcile_catalog(CATALOG.to_string(), ResolveStrategy::UseLatest, 1)
        .await
        .unwrap();
    let mut procedure_watcher = cluster
        .metasrv
        .procedure_manager()
        .procedure_watcher(catalog_procedure_id)
        .unwrap();
    watcher::wait(&mut procedure_watcher).await.unwrap();
    let catalog_procedure_id = catalog_procedure_id.to_string();

    assert_eventually_eq(
        &frontend,
        &format!(
            "SELECT count(*) AS event_count, \
                 json_get_int(payload, 'version') AS version, \
                 json_get_string(payload, 'resolve_strategy') AS resolve_strategy, \
                 json_get_bool(payload, 'fail_fast') AS fail_fast, \
                 json_get_int(payload, 'parallelism') AS parallelism \
             FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_catalog' AND procedure_id = '{catalog_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'Submitted' \
             AND catalog_name = '{CATALOG}' AND schema_name IS NULL AND table_name IS NULL \
             AND table_id IS NULL AND physical_table_id IS NULL \
             GROUP BY json_get_int(payload, 'version'), \
                 json_get_string(payload, 'resolve_strategy'), \
                 json_get_bool(payload, 'fail_fast'), \
                 json_get_int(payload, 'parallelism')"
        ),
        "\
+-------------+---------+------------------+-----------+-------------+
| event_count | version | resolve_strategy | fail_fast | parallelism |
+-------------+---------+------------------+-----------+-------------+
| 1           | 1       | use_latest       | false     | 1           |
+-------------+---------+------------------+-----------+-------------+",
    )
    .await;

    let catalog_result_payload = find_eventually_string(
        &frontend,
        &format!(
            "SELECT json_to_string(payload) AS payload FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_catalog' AND procedure_id = '{catalog_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'Succeeded' \
             AND catalog_name = '{CATALOG}' AND schema_name IS NULL AND table_name IS NULL \
             AND table_id IS NULL AND physical_table_id IS NULL LIMIT 1"
        ),
        "payload",
    )
    .await;
    let catalog_result: serde_json::Value = serde_json::from_str(&catalog_result_payload).unwrap();
    assert_eq!(catalog_result["version"], serde_json::json!(1));
    assert_eq!(catalog_result["complete"], serde_json::json!(true));
    let processed_database_count = catalog_result["processed_database_count"].as_u64().unwrap();
    let succeeded_database_count = catalog_result["succeeded_database_count"].as_u64().unwrap();
    let failed_database_count = catalog_result["failed_database_count"].as_u64().unwrap();
    assert_eq!(
        processed_database_count,
        succeeded_database_count + failed_database_count
    );
    assert!(processed_database_count > 0);
    assert_eq!(failed_database_count, 0);

    let database_procedure_id = find_eventually_string(
        &frontend,
        &format!(
            "SELECT procedure_id FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_database' AND catalog_name = '{CATALOG}' \
             AND schema_name = '{DATABASE}' \
             AND json_get_string(procedure_trigger, 'type') = 'Submitted' LIMIT 1"
        ),
        "procedure_id",
    )
    .await;

    assert_eventually_eq(
        &frontend,
        &format!(
            "SELECT parent.procedure_id AS parent_procedure_id, \
                 child.procedure_id AS child_procedure_id, \
                 parent.type AS parent_event_type, \
                 child.type AS child_event_type, \
                 parent.catalog_name AS parent_catalog_name, \
                 parent.schema_name AS parent_schema_name, \
                 child.catalog_name AS child_catalog_name, \
                 child.schema_name AS child_schema_name \
             FROM {EVENTS_TABLE} AS parent \
             JOIN {EVENTS_TABLE} AS child \
               ON json_get_string(parent.procedure_trigger, 'procedure_id') = child.procedure_id \
             WHERE parent.procedure_id = '{catalog_procedure_id}' \
             AND json_get_string(parent.procedure_trigger, 'type') = 'ChildSubmitted' \
             AND child.procedure_id = '{database_procedure_id}' \
             AND json_get_string(child.procedure_trigger, 'type') = 'Submitted'"
        ),
        &format!(
            "\
+--------------------------------------+--------------------------------------+-------------------+--------------------+---------------------+--------------------+--------------------+-------------------------------+
| parent_procedure_id                  | child_procedure_id                   | parent_event_type | child_event_type   | parent_catalog_name | parent_schema_name | child_catalog_name | child_schema_name             |
+--------------------------------------+--------------------------------------+-------------------+--------------------+---------------------+--------------------+--------------------+-------------------------------+
| {catalog_procedure_id} | {database_procedure_id} | reconcile_catalog | reconcile_database | greptime            |                    | greptime           | reconciliation_event_database |
+--------------------------------------+--------------------------------------+-------------------+--------------------+---------------------+--------------------+--------------------+-------------------------------+"
        ),
    )
    .await;

    assert_eventually_eq(
        &frontend,
        &format!(
            "SELECT count(*) AS event_count, \
                 json_get_string(procedure_trigger, 'outcome') AS outcome \
             FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_catalog' AND procedure_id = '{catalog_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'ChildSubmitted' \
             AND json_get_string(procedure_trigger, 'procedure_id') = '{database_procedure_id}' \
             AND catalog_name = '{CATALOG}' AND schema_name IS NULL \
             AND json_is_null(payload) \
             GROUP BY json_get_string(procedure_trigger, 'outcome')"
        ),
        "\
+-------------+----------+
| event_count | outcome  |
+-------------+----------+
| 1           | Accepted |
+-------------+----------+",
    )
    .await;

    assert_eventually_eq(
        &frontend,
        &format!(
            "SELECT count(*) AS event_count, \
                 json_get_int(payload, 'version') AS version, \
                 json_get_string(payload, 'resolve_strategy') AS resolve_strategy, \
                 json_get_bool(payload, 'fail_fast') AS fail_fast, \
                 json_get_int(payload, 'parallelism') AS parallelism, \
                 json_get_bool(payload, 'is_subprocedure') AS is_subprocedure \
             FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_database' AND procedure_id = '{database_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'Submitted' \
             AND catalog_name = '{CATALOG}' AND schema_name = '{DATABASE}' \
             AND table_name IS NULL AND table_id IS NULL AND physical_table_id IS NULL \
             GROUP BY json_get_int(payload, 'version'), \
                 json_get_string(payload, 'resolve_strategy'), \
                 json_get_bool(payload, 'fail_fast'), \
                 json_get_int(payload, 'parallelism'), \
                 json_get_bool(payload, 'is_subprocedure')"
        ),
        "\
+-------------+---------+------------------+-----------+-------------+-----------------+
| event_count | version | resolve_strategy | fail_fast | parallelism | is_subprocedure |
+-------------+---------+------------------+-----------+-------------+-----------------+
| 1           | 1       | use_latest       | false     | 1           | true            |
+-------------+---------+------------------+-----------+-------------+-----------------+",
    )
    .await;

    assert_eventually_eq(
        &frontend,
        &format!(
            "SELECT count(*) AS event_count, \
                 json_get_int(payload, 'version') AS version, \
                 json_get_bool(payload, 'complete') AS complete, \
                 json_get_int(payload, 'processed_table_count') AS processed_count, \
                 json_get_int(payload, 'succeeded_table_count') AS succeeded_count, \
                 json_get_int(payload, 'failed_table_count') AS failed_count, \
                 json_get_int(payload, 'succeeded_subprocedure_count') AS succeeded_child_count, \
                 json_get_int(payload, 'failed_subprocedure_count') AS failed_child_count \
             FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_database' AND procedure_id = '{database_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'Succeeded' \
             AND catalog_name = '{CATALOG}' AND schema_name = '{DATABASE}' \
             AND table_name IS NULL AND table_id IS NULL AND physical_table_id IS NULL \
             GROUP BY json_get_int(payload, 'version'), \
                 json_get_bool(payload, 'complete'), \
                 json_get_int(payload, 'processed_table_count'), \
                 json_get_int(payload, 'succeeded_table_count'), \
                 json_get_int(payload, 'failed_table_count'), \
                 json_get_int(payload, 'succeeded_subprocedure_count'), \
                 json_get_int(payload, 'failed_subprocedure_count')"
        ),
        "\
+-------------+---------+----------+-----------------+-----------------+--------------+-----------------------+--------------------+
| event_count | version | complete | processed_count | succeeded_count | failed_count | succeeded_child_count | failed_child_count |
+-------------+---------+----------+-----------------+-----------------+--------------+-----------------------+--------------------+
| 1           | 1       | true     | 1               | 1               | 0            | 1                     | 0                  |
+-------------+---------+----------+-----------------+-----------------+--------------+-----------------------+--------------------+",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_reconciliation_events() {
    common_telemetry::init_default_ut_logging();

    let cluster = GreptimeDbClusterBuilder::new("table_reconciliation_events")
        .await
        .with_datanodes(1)
        .build(true)
        .await;
    let frontend = cluster.fe_instance().clone();
    run_sql(frontend.as_ref(), &format!("CREATE DATABASE {DATABASE}")).await;
    run_sql(
        frontend.as_ref(),
        &format!("CREATE TABLE {DATABASE}.metrics (ts TIMESTAMP TIME INDEX)"),
    )
    .await;

    let table_procedure_id = cluster
        .metasrv
        .reconciliation_manager()
        .reconcile_table(
            TableReference {
                catalog: CATALOG,
                schema: DATABASE,
                table: "metrics",
            },
            ResolveStrategy::UseLatest,
        )
        .await
        .unwrap();
    let mut procedure_watcher = cluster
        .metasrv
        .procedure_manager()
        .procedure_watcher(table_procedure_id)
        .unwrap();
    watcher::wait(&mut procedure_watcher).await.unwrap();
    let table_procedure_id = table_procedure_id.to_string();

    assert_eventually_eq(
        &frontend,
        &format!(
            "SELECT count(*) = 1 AS matches FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_table' AND procedure_id = '{table_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'Submitted' \
             AND catalog_name = '{CATALOG}' AND schema_name = '{DATABASE}' \
             AND table_name = 'metrics' AND table_id IS NOT NULL \
             AND physical_table_id IS NULL \
             AND json_get_int(payload, 'version') = 1 \
             AND json_get_string(payload, 'resolve_strategy') = 'use_latest' \
             AND json_get_bool(payload, 'is_subprocedure') = false"
        ),
        "\
+---------+
| matches |
+---------+
| true    |
+---------+",
    )
    .await;

    let table_result_payload = find_eventually_string(
        &frontend,
        &format!(
            "SELECT json_to_string(payload) AS payload FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_table' AND procedure_id = '{table_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'Succeeded' \
             AND catalog_name = '{CATALOG}' AND schema_name = '{DATABASE}' \
             AND table_name = 'metrics' AND table_id IS NOT NULL \
             AND physical_table_id IS NULL LIMIT 1"
        ),
        "payload",
    )
    .await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&table_result_payload).unwrap(),
        serde_json::json!({
            "version": 1,
            "complete": true,
            "metadata_state": "consistent",
            "resolution_strategy_applied": null,
            "resolved_column_count": 1,
            "scanned_region_count": 1,
            "updated_region_count": 0,
            "table_info_updated": true,
            "last_completed_phase": "update_table_info",
        })
    );

    assert_eventually_eq(
        &frontend,
        &format!(
            "SELECT count(*) AS terminal_event_count FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_table' AND procedure_id = '{table_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'Succeeded' \
             AND catalog_name = '{CATALOG}' AND schema_name = '{DATABASE}' \
             AND table_name = 'metrics' AND table_id IS NOT NULL \
             AND physical_table_id IS NULL"
        ),
        "\
+----------------------+
| terminal_event_count |
+----------------------+
| 1                    |
+----------------------+",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_logical_table_reconciliation_events() {
    common_telemetry::init_default_ut_logging();

    let cluster = GreptimeDbClusterBuilder::new("logical_table_reconciliation_events")
        .await
        .with_datanodes(1)
        .build(true)
        .await;
    let frontend = cluster.fe_instance().clone();
    run_sql(frontend.as_ref(), &format!("CREATE DATABASE {DATABASE}")).await;

    run_sql(
        frontend.as_ref(),
        &format!(
            "CREATE TABLE {DATABASE}.metric_physical \
             (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric \
             WITH (\"physical_metric_table\" = \"\")"
        ),
    )
    .await;
    run_sql(
        frontend.as_ref(),
        &format!(
            "CREATE TABLE {DATABASE}.logical_cpu \
             (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) ENGINE=metric \
             WITH (\"on_physical_table\" = \"metric_physical\")"
        ),
    )
    .await;
    run_sql(
        frontend.as_ref(),
        &format!(
            "CREATE TABLE {DATABASE}.logical_memory \
             (ts TIMESTAMP TIME INDEX, val DOUBLE, job STRING PRIMARY KEY) ENGINE=metric \
             WITH (\"on_physical_table\" = \"metric_physical\")"
        ),
    )
    .await;

    let parent_procedure_id = cluster
        .metasrv
        .reconciliation_manager()
        .reconcile_database(
            CATALOG.to_string(),
            DATABASE.to_string(),
            ResolveStrategy::UseLatest,
            2,
        )
        .await
        .unwrap();
    let mut procedure_watcher = cluster
        .metasrv
        .procedure_manager()
        .procedure_watcher(parent_procedure_id)
        .unwrap();
    watcher::wait(&mut procedure_watcher).await.unwrap();

    let logical_procedure_id = find_eventually_string(
        &frontend,
        &format!(
            "SELECT procedure_id FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_logical_tables' AND catalog_name = '{CATALOG}' \
             AND schema_name = '{DATABASE}' AND table_name = 'logical_cpu' \
             AND json_get_string(procedure_trigger, 'type') = 'Submitted' LIMIT 1"
        ),
        "procedure_id",
    )
    .await;

    assert_eventually_eq(
        &frontend,
        &format!(
            "SELECT count(*) AS event_count, \
                 count(DISTINCT table_id) AS table_id_count, \
                 count(DISTINCT physical_table_id) AS physical_table_id_count, \
                 json_get_int(payload, 'version') AS version, \
                 json_get_int(payload, 'logical_table_count') AS logical_table_count, \
                 json_get_bool(payload, 'is_subprocedure') AS is_subprocedure \
             FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_logical_tables' \
             AND procedure_id = '{logical_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'Submitted' \
             AND catalog_name = '{CATALOG}' AND schema_name = '{DATABASE}' \
             AND table_name IN ('logical_cpu', 'logical_memory') \
             AND table_id IS NOT NULL AND physical_table_id IS NOT NULL \
             AND table_id != physical_table_id \
             GROUP BY json_get_int(payload, 'version'), \
                 json_get_int(payload, 'logical_table_count'), \
                 json_get_bool(payload, 'is_subprocedure')"
        ),
        "\
+-------------+----------------+-------------------------+---------+---------------------+-----------------+
| event_count | table_id_count | physical_table_id_count | version | logical_table_count | is_subprocedure |
+-------------+----------------+-------------------------+---------+---------------------+-----------------+
| 2           | 2              | 1                       | 1       | 2                   | true            |
+-------------+----------------+-------------------------+---------+---------------------+-----------------+",
    )
    .await;

    assert_eventually_eq(
        &frontend,
        &format!(
            "SELECT count(*) AS event_count, \
                 count(DISTINCT table_id) AS table_id_count, \
                 count(DISTINCT physical_table_id) AS physical_table_id_count, \
                 json_get_int(payload, 'version') AS version, \
                 json_get_bool(payload, 'complete') AS complete, \
                 json_get_int(payload, 'processed_table_count') AS processed_count, \
                 json_get_int(payload, 'metadata_consistent_table_count') AS consistent_count, \
                 json_get_int(payload, 'metadata_inconsistent_table_count') AS inconsistent_count, \
                 json_get_int(payload, 'create_table_count') AS create_count, \
                 json_get_int(payload, 'update_table_info_count') AS update_count \
             FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_logical_tables' \
             AND procedure_id = '{logical_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'Succeeded' \
             AND catalog_name = '{CATALOG}' AND schema_name = '{DATABASE}' \
             AND table_name IN ('logical_cpu', 'logical_memory') \
             GROUP BY json_get_int(payload, 'version'), \
                 json_get_bool(payload, 'complete'), \
                 json_get_int(payload, 'processed_table_count'), \
                 json_get_int(payload, 'metadata_consistent_table_count'), \
                 json_get_int(payload, 'metadata_inconsistent_table_count'), \
                 json_get_int(payload, 'create_table_count'), \
                 json_get_int(payload, 'update_table_info_count')"
        ),
        "\
+-------------+----------------+-------------------------+---------+----------+-----------------+------------------+--------------------+--------------+--------------+
| event_count | table_id_count | physical_table_id_count | version | complete | processed_count | consistent_count | inconsistent_count | create_count | update_count |
+-------------+----------------+-------------------------+---------+----------+-----------------+------------------+--------------------+--------------+--------------+
| 2           | 2              | 1                       | 1       | true     | 2               | 2                | 0                  | 0            | 0            |
+-------------+----------------+-------------------------+---------+----------+-----------------+------------------+--------------------+--------------+--------------+",
    )
    .await;
}

async fn run_sql(instance: &Instance, sql: &str) {
    let output = SqlQueryHandler::do_query(instance, sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    assert!(matches!(output.data, OutputData::AffectedRows(_)), "{sql}");
}
