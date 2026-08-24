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
            "SELECT count(*) = 2 AND count(DISTINCT table_id) = 2 \
                 AND count(DISTINCT physical_table_id) = 1 AS matches \
             FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_logical_tables' \
             AND procedure_id = '{logical_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'Submitted' \
             AND catalog_name = '{CATALOG}' AND schema_name = '{DATABASE}' \
             AND table_name IN ('logical_cpu', 'logical_memory') \
             AND table_id IS NOT NULL AND physical_table_id IS NOT NULL \
             AND table_id != physical_table_id \
             AND json_get_int(payload, 'version') = 1 \
             AND json_get_int(payload, 'logical_table_count') = 2 \
             AND json_get_bool(payload, 'is_subprocedure') = true"
        ),
        "\
+---------+
| matches |
+---------+
| true    |
+---------+",
    )
    .await;

    assert_eventually_eq(
        &frontend,
        &format!(
            "SELECT count(*) = 2 AND count(DISTINCT table_id) = 2 \
                 AND count(DISTINCT physical_table_id) = 1 AS matches \
             FROM {EVENTS_TABLE} \
             WHERE type = 'reconcile_logical_tables' \
             AND procedure_id = '{logical_procedure_id}' \
             AND json_get_string(procedure_trigger, 'type') = 'Succeeded' \
             AND catalog_name = '{CATALOG}' AND schema_name = '{DATABASE}' \
             AND table_name IN ('logical_cpu', 'logical_memory') \
             AND json_get_bool(payload, 'complete') = true \
             AND json_get_int(payload, 'processed_table_count') = 2 \
             AND json_get_int(payload, 'metadata_consistent_table_count') = 2 \
             AND json_get_int(payload, 'metadata_inconsistent_table_count') = 0 \
             AND json_get_int(payload, 'missing_region_table_count') = 0 \
             AND json_get_int(payload, 'resolved_column_count') = 6 \
             AND json_get_int(payload, 'scanned_region_count') = 2 \
             AND json_get_int(payload, 'created_region_table_count') = 0 \
             AND json_get_int(payload, 'created_region_count') = 0 \
             AND json_get_int(payload, 'updated_table_info_count') = 0 \
             AND json_get_string(payload, 'last_completed_phase') = 'update_table_infos'"
        ),
        "\
+---------+
| matches |
+---------+
| true    |
+---------+",
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
