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

use std::time::Duration;

use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, OutputData};
use common_meta::reconciliation::ResolveStrategy;
use common_meta::reconciliation::manager::ReconciliationManagerRef;
use common_procedure::ProcedureManagerRef;
use common_recordbatch::util::collect_batches;
use common_test_util::recordbatch::check_output_stream;
use table::table_reference::TableReference;

use crate::cluster::GreptimeDbClusterBuilder;
use crate::tests::test_util::{
    MockInstance, MockInstanceBuilder, RebuildableMockInstance, TestContext, dump_kvbackend,
    execute_sql, restore_kvbackend, try_execute_sql, wait_procedure,
};

const CREATE_MONITOR_TABLE_SQL: &str = r#"
CREATE TABLE monitor (
    t TIMESTAMP TIME INDEX,
    env STRING INVERTED INDEX,
    cloud_provider STRING,
    latency Float,
    PRIMARY KEY (env, cloud_provider)
)
PARTITION ON COLUMNS (`env`) (
    `env` < 'env-0',
    `env` >= 'env-0' AND `env` < 'env-1',
    `env` >= 'env-1' AND `env` < 'env-2',
    `env` >= 'env-2'
)
with('append_mode'='true');"#;

const CREATE_TABLE_SQL: &str = r#"
CREATE TABLE grpc_latencies (
    ts TIMESTAMP TIME INDEX,
    host STRING INVERTED INDEX,
    method_name STRING,
    env STRING NOT NULL,
    latency Float,
    PRIMARY KEY (host, method_name)
)
PARTITION ON COLUMNS (`host`) (
    `host` < 'host1',
    `host` >= 'host1' AND `host` < 'host2',
    `host` >= 'host2' AND `host` < 'host3',
    `host` >= 'host3'
)
with('append_mode'='true');"#;

const INSERT_DATA_SQL: &str = r#"
INSERT INTO grpc_latencies (
    ts,
    host,
    method_name,
    latency,
    env
) VALUES (
    '2025-08-08 20:00:06',
    'host1',
    'GetUser',
    103.0,
    'prod'
);
"#;

const INSERT_DATA_SQL_WITHOUT_ENV: &str = r#"
INSERT INTO grpc_latencies (
    ts,
    host,
    method_name,
    latency
) VALUES (
    '2025-08-08 20:00:07',
    'host2',
    'GetUser',
    104.0
);
"#;

const RENAME_TABLE_SQL: &str = r#"
ALTER TABLE grpc_latencies RENAME grpc_latencies_renamed;
"#;

const MODIFY_COLUMN_LATENCY_TYPE_SQL: &str = r#"
ALTER TABLE grpc_latencies MODIFY COLUMN latency DOUBLE;
"#;

const INSERT_DATA_SQL_WITH_CLOUD_PROVIDER: &str = r#"
INSERT INTO grpc_latencies (
    ts,
    host,
    method_name,
    cloud_provider,
    latency,
) VALUES (
    '2025-08-08 20:00:08',
    'host3',
    'GetUser',
    'aws',
    105.0
);
"#;

const ADD_COLUMN_CLOUD_PROVIDER_SQL: &str = r#"
ALTER TABLE grpc_latencies ADD COLUMN cloud_provider STRING;
"#;

const ADD_COLUMN_ENV_SQL: &str = r#"
ALTER TABLE grpc_latencies ADD COLUMN env STRING;
"#;

const DROP_COLUMN_ENV_SQL: &str = r#"
ALTER TABLE grpc_latencies DROP COLUMN env;
"#;

#[tokio::test]
async fn test_reconcile_dropped_column() {
    let builder = GreptimeDbClusterBuilder::new("test_reconcile_dropped_column").await;
    let mut test_context = TestContext::new(MockInstanceBuilder::Distributed(builder)).await;

    // Create the table.
    let output = execute_sql(&test_context.frontend(), CREATE_TABLE_SQL).await;
    assert_affected_rows(output.data).await;

    // Insert data.
    let output = execute_sql(&test_context.frontend(), INSERT_DATA_SQL).await;
    assert_affected_rows(output.data).await;

    let keyvalues = dump_kvbackend(test_context.metasrv().kv_backend()).await;

    // Drop column env.
    let output = execute_sql(&test_context.frontend(), DROP_COLUMN_ENV_SQL).await;
    assert_affected_rows(output.data).await;

    let output = execute_sql(&test_context.frontend(), INSERT_DATA_SQL_WITHOUT_ENV).await;
    assert_affected_rows(output.data).await;

    restore_kvbackend(test_context.metasrv().kv_backend(), keyvalues).await;

    test_context.rebuild().await;
    let frontend = test_context.frontend();
    let metasrv = test_context.metasrv();
    let reconciliation_manager = metasrv.reconciliation_manager();

    // We should unable to query table due to the column env is dropped.
    let output = execute_sql(&frontend, "SELECT * FROM grpc_latencies").await;
    assert_no_field_error(output.data).await;

    // Reconcile the table.
    reconcile_table(
        metasrv.procedure_manager(),
        reconciliation_manager,
        "grpc_latencies",
    )
    .await;
    // Try best effort to wait for the cache to be invalidated.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Now we should able to query table again.
    let output = execute_sql(&frontend, "SELECT * FROM grpc_latencies ORDER BY host").await;
    let expected = r#"+---------------------+-------+-------------+---------+
| ts                  | host  | method_name | latency |
+---------------------+-------+-------------+---------+
| 2025-08-08T20:00:06 | host1 | GetUser     | 103.0   |
| 2025-08-08T20:00:07 | host2 | GetUser     | 104.0   |
+---------------------+-------+-------------+---------+"#;
    check_output_stream(output.data, expected).await;

    // Add column env.
    let output = execute_sql(&frontend, ADD_COLUMN_ENV_SQL).await;
    assert_affected_rows(output.data).await;

    // Query table again.
    let output = execute_sql(&frontend, "SELECT * FROM grpc_latencies ORDER BY host").await;
    let expected = r#"+---------------------+-------+-------------+---------+-----+
| ts                  | host  | method_name | latency | env |
+---------------------+-------+-------------+---------+-----+
| 2025-08-08T20:00:06 | host1 | GetUser     | 103.0   |     |
| 2025-08-08T20:00:07 | host2 | GetUser     | 104.0   |     |
+---------------------+-------+-------------+---------+-----+"#;
    check_output_stream(output.data, expected).await;

    // Drop column env.
    let output = execute_sql(&frontend, DROP_COLUMN_ENV_SQL).await;
    assert_affected_rows(output.data).await;

    // Add column cloud_provider.
    let output = execute_sql(&frontend, ADD_COLUMN_CLOUD_PROVIDER_SQL).await;
    assert_affected_rows(output.data).await;

    // Inserts data with cloud_provider.
    let output = execute_sql(&frontend, INSERT_DATA_SQL_WITH_CLOUD_PROVIDER).await;
    assert_affected_rows(output.data).await;

    // Now we should able to query table again.
    let output = execute_sql(&frontend, "SELECT * FROM grpc_latencies ORDER BY host").await;
    let expected = r#"+---------------------+-------+-------------+---------+----------------+
| ts                  | host  | method_name | latency | cloud_provider |
+---------------------+-------+-------------+---------+----------------+
| 2025-08-08T20:00:06 | host1 | GetUser     | 103.0   |                |
| 2025-08-08T20:00:07 | host2 | GetUser     | 104.0   |                |
| 2025-08-08T20:00:08 | host3 | GetUser     | 105.0   | aws            |
+---------------------+-------+-------------+---------+----------------+"#;
    check_output_stream(output.data, expected).await;
}

#[tokio::test]
async fn test_reconcile_added_column() {
    let builder = GreptimeDbClusterBuilder::new("test_reconcile_added_column").await;
    let mut test_context = TestContext::new(MockInstanceBuilder::Distributed(builder)).await;

    // Create the table.
    let output = execute_sql(&test_context.frontend(), CREATE_TABLE_SQL).await;
    assert_affected_rows(output.data).await;

    // Insert data.
    let output = execute_sql(&test_context.frontend(), INSERT_DATA_SQL).await;
    assert_affected_rows(output.data).await;

    // Drop column env.
    let output = execute_sql(&test_context.frontend(), DROP_COLUMN_ENV_SQL).await;
    assert_affected_rows(output.data).await;

    let keyvalues = dump_kvbackend(test_context.metasrv().kv_backend()).await;

    // Add column cloud_provider.
    let output = execute_sql(&test_context.frontend(), ADD_COLUMN_CLOUD_PROVIDER_SQL).await;
    assert_affected_rows(output.data).await;

    let output = execute_sql(
        &test_context.frontend(),
        INSERT_DATA_SQL_WITH_CLOUD_PROVIDER,
    )
    .await;
    assert_affected_rows(output.data).await;

    restore_kvbackend(test_context.metasrv().kv_backend(), keyvalues).await;

    test_context.rebuild().await;
    let frontend = test_context.frontend();
    let metasrv = test_context.metasrv();
    let reconciliation_manager = metasrv.reconciliation_manager();

    // The column cloud_provider is missing.
    let output = execute_sql(&frontend, "SELECT * FROM grpc_latencies ORDER BY host").await;
    let expected = r#"+---------------------+-------+-------------+---------+
| ts                  | host  | method_name | latency |
+---------------------+-------+-------------+---------+
| 2025-08-08T20:00:06 | host1 | GetUser     | 103.0   |
| 2025-08-08T20:00:08 | host3 | GetUser     | 105.0   |
+---------------------+-------+-------------+---------+"#;
    check_output_stream(output.data, expected).await;

    // Reconcile the table.
    reconcile_table(
        metasrv.procedure_manager(),
        reconciliation_manager,
        "grpc_latencies",
    )
    .await;
    // Try best effort to wait for the cache to be invalidated.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Now the column cloud_provider is available.
    let output = execute_sql(&frontend, "SELECT * FROM grpc_latencies ORDER BY host").await;
    let expected = r#"+---------------------+-------+-------------+---------+----------------+
| ts                  | host  | method_name | latency | cloud_provider |
+---------------------+-------+-------------+---------+----------------+
| 2025-08-08T20:00:06 | host1 | GetUser     | 103.0   |                |
| 2025-08-08T20:00:08 | host3 | GetUser     | 105.0   | aws            |
+---------------------+-------+-------------+---------+----------------+"#;
    check_output_stream(output.data, expected).await;

    // Add column env.
    let output = execute_sql(&frontend, ADD_COLUMN_ENV_SQL).await;
    assert_affected_rows(output.data).await;

    // Insert data with env.
    let output = execute_sql(&frontend, INSERT_DATA_SQL).await;
    assert_affected_rows(output.data).await;

    let output = execute_sql(&frontend, "SELECT * FROM grpc_latencies ORDER BY host, env").await;
    let expected = r#"+---------------------+-------+-------------+---------+----------------+------+
| ts                  | host  | method_name | latency | cloud_provider | env  |
+---------------------+-------+-------------+---------+----------------+------+
| 2025-08-08T20:00:06 | host1 | GetUser     | 103.0   |                | prod |
| 2025-08-08T20:00:06 | host1 | GetUser     | 103.0   |                |      |
| 2025-08-08T20:00:08 | host3 | GetUser     | 105.0   | aws            |      |
+---------------------+-------+-------------+---------+----------------+------+"#;
    check_output_stream(output.data, expected).await;
}

#[tokio::test]
async fn test_reconcile_modify_column_type() {
    let builder = GreptimeDbClusterBuilder::new("test_reconcile_added_column").await;
    let mut test_context = TestContext::new(MockInstanceBuilder::Distributed(builder)).await;

    // Create the table.
    let output = execute_sql(&test_context.frontend(), CREATE_TABLE_SQL).await;
    assert_affected_rows(output.data).await;

    // Insert data.
    let output = execute_sql(&test_context.frontend(), INSERT_DATA_SQL).await;
    assert_affected_rows(output.data).await;

    // Drop column env.
    let output = execute_sql(&test_context.frontend(), DROP_COLUMN_ENV_SQL).await;
    assert_affected_rows(output.data).await;

    let keyvalues = dump_kvbackend(test_context.metasrv().kv_backend()).await;

    // Add column cloud_provider.
    let output = execute_sql(&test_context.frontend(), MODIFY_COLUMN_LATENCY_TYPE_SQL).await;
    assert_affected_rows(output.data).await;

    let output = execute_sql(&test_context.frontend(), INSERT_DATA_SQL_WITHOUT_ENV).await;
    assert_affected_rows(output.data).await;

    restore_kvbackend(test_context.metasrv().kv_backend(), keyvalues).await;

    test_context.rebuild().await;
    let frontend = test_context.frontend();
    let metasrv = test_context.metasrv();
    let reconciliation_manager = metasrv.reconciliation_manager();

    // The column cloud_provider is missing.
    let output = execute_sql(&frontend, "SELECT * FROM grpc_latencies ORDER BY host").await;
    assert_has_different_type(output.data).await;

    // Reconcile the table.
    reconcile_table(
        metasrv.procedure_manager(),
        reconciliation_manager,
        "grpc_latencies",
    )
    .await;
    // Try best effort to wait for the cache to be invalidated.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Now we can query the table again.
    let output = execute_sql(&frontend, "SELECT * FROM grpc_latencies ORDER BY host").await;
    let expected = r#"+---------------------+-------+-------------+---------+
| ts                  | host  | method_name | latency |
+---------------------+-------+-------------+---------+
| 2025-08-08T20:00:06 | host1 | GetUser     | 103.0   |
| 2025-08-08T20:00:07 | host2 | GetUser     | 104.0   |
+---------------------+-------+-------------+---------+"#;
    check_output_stream(output.data, expected).await;

    // Add column env.
    let output = execute_sql(&frontend, ADD_COLUMN_CLOUD_PROVIDER_SQL).await;
    assert_affected_rows(output.data).await;

    // Insert data with env.
    let output = execute_sql(&frontend, INSERT_DATA_SQL_WITH_CLOUD_PROVIDER).await;
    assert_affected_rows(output.data).await;

    let output = execute_sql(&frontend, "SELECT * FROM grpc_latencies ORDER BY host").await;
    let expected = r#"+---------------------+-------+-------------+---------+----------------+
| ts                  | host  | method_name | latency | cloud_provider |
+---------------------+-------+-------------+---------+----------------+
| 2025-08-08T20:00:06 | host1 | GetUser     | 103.0   |                |
| 2025-08-08T20:00:07 | host2 | GetUser     | 104.0   |                |
| 2025-08-08T20:00:08 | host3 | GetUser     | 105.0   | aws            |
+---------------------+-------+-------------+---------+----------------+"#;
    check_output_stream(output.data, expected).await;
}

#[tokio::test]
async fn test_recover_metadata_failed() {
    let builder = GreptimeDbClusterBuilder::new("test_recover_metadata_failed")
        .await
        .with_datanodes(1);
    let mut test_context = TestContext::new(MockInstanceBuilder::Distributed(builder)).await;

    // Backup the kv backend.
    let keyvalues = dump_kvbackend(test_context.metasrv().kv_backend()).await;

    let output = execute_sql(&test_context.frontend(), CREATE_TABLE_SQL).await;
    assert_affected_rows(output.data).await;

    let output = execute_sql(&test_context.frontend(), INSERT_DATA_SQL).await;
    assert_affected_rows(output.data).await;

    let output = execute_sql(&test_context.frontend(), "SELECT * FROM grpc_latencies").await;
    let expected = r#"+---------------------+-------+-------------+------+---------+
| ts                  | host  | method_name | env  | latency |
+---------------------+-------+-------------+------+---------+
| 2025-08-08T20:00:06 | host1 | GetUser     | prod | 103.0   |
+---------------------+-------+-------------+------+---------+"#;
    check_output_stream(output.data, expected).await;

    restore_kvbackend(test_context.metasrv().kv_backend(), keyvalues.clone()).await;
    test_context.rebuild().await;

    // Only grpc_latencies table is visible.
    let output = execute_sql(&test_context.frontend(), "show tables;").await;
    let expected = r#"+------------------+
| Tables_in_public |
+------------------+
| numbers          |
+------------------+"#;
    check_output_stream(output.data, expected).await;

    // Expect table creation to fail because the region directory already exists.
    let error = try_execute_sql(&test_context.frontend(), CREATE_MONITOR_TABLE_SQL)
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("recovered metadata has different schema"));
}

#[tokio::test]
async fn test_set_table_id() {
    let builder = GreptimeDbClusterBuilder::new("test_set_table_id").await;
    let mut test_context = TestContext::new(MockInstanceBuilder::Distributed(builder)).await;

    let keyvalues = dump_kvbackend(test_context.metasrv().kv_backend()).await;
    let output = execute_sql(&test_context.frontend(), CREATE_TABLE_SQL).await;
    assert_affected_rows(output.data).await;
    restore_kvbackend(test_context.metasrv().kv_backend(), keyvalues).await;
    test_context.rebuild().await;

    let metasrv = test_context.metasrv();
    // Due to the table id 1024 already allocated, we need to jump to 1025.
    metasrv.table_id_sequence().jump_to(1025).await.unwrap();

    // We should able to create table now.
    let output = execute_sql(&test_context.frontend(), CREATE_MONITOR_TABLE_SQL).await;
    assert_affected_rows(output.data).await;
}

#[tokio::test]
async fn test_dropped_table() {
    let builder = GreptimeDbClusterBuilder::new("test_dropped_table").await;
    let mut test_context = TestContext::new(MockInstanceBuilder::Distributed(builder)).await;

    // Creates the table.
    let output = execute_sql(&test_context.frontend(), CREATE_TABLE_SQL).await;
    assert_affected_rows(output.data).await;

    // Insert data.
    let output = execute_sql(&test_context.frontend(), INSERT_DATA_SQL).await;
    assert_affected_rows(output.data).await;

    // Backup the kv backend.
    let keyvalues = dump_kvbackend(test_context.metasrv().kv_backend()).await;

    // Drop the table.
    let output = execute_sql(&test_context.frontend(), "DROP TABLE grpc_latencies").await;
    assert_affected_rows(output.data).await;

    restore_kvbackend(test_context.metasrv().kv_backend(), keyvalues).await;
    // Enable the recovery mode, so the datanode will ignore empty region directory during recovery.
    test_context
        .metasrv()
        .runtime_switch_manager()
        .set_recovery_mode()
        .await
        .unwrap();
    test_context.rebuild().await;

    let output = execute_sql(&test_context.frontend(), "show tables;").await;
    let expected = r#"+------------------+
| Tables_in_public |
+------------------+
| grpc_latencies   |
| numbers          |
+------------------+"#;
    check_output_stream(output.data, expected).await;

    // We can't query the table because the table is dropped.
    let output = execute_sql(&test_context.frontend(), "SELECT * FROM grpc_latencies").await;
    region_not_found(output.data).await;

    // We should able to drop the table.
    let output = execute_sql(&test_context.frontend(), "DROP TABLE grpc_latencies").await;
    assert_affected_rows(output.data).await;
}

#[tokio::test]
async fn test_renamed_table() {
    let builder = GreptimeDbClusterBuilder::new("test_renamed_table").await;
    let mut test_context = TestContext::new(MockInstanceBuilder::Distributed(builder)).await;

    // Creates the table.
    let output = execute_sql(&test_context.frontend(), CREATE_TABLE_SQL).await;
    assert_affected_rows(output.data).await;

    // Inserts data.
    let output = execute_sql(&test_context.frontend(), INSERT_DATA_SQL).await;
    assert_affected_rows(output.data).await;

    // Backup the kv backend.
    let keyvalues = dump_kvbackend(test_context.metasrv().kv_backend()).await;

    // Renames the table.
    let output = execute_sql(&test_context.frontend(), RENAME_TABLE_SQL).await;
    assert_affected_rows(output.data).await;

    let output = execute_sql(
        &test_context.frontend(),
        "SELECT * FROM grpc_latencies_renamed",
    )
    .await;
    let expected = r#"+---------------------+-------+-------------+------+---------+
| ts                  | host  | method_name | env  | latency |
+---------------------+-------+-------------+------+---------+
| 2025-08-08T20:00:06 | host1 | GetUser     | prod | 103.0   |
+---------------------+-------+-------------+------+---------+"#;
    check_output_stream(output.data, expected).await;

    restore_kvbackend(test_context.metasrv().kv_backend(), keyvalues).await;
    test_context.rebuild().await;

    // After restoring the metadata, only the table with its original name (before the rename) is visible.
    let output = execute_sql(&test_context.frontend(), "SELECT * FROM grpc_latencies").await;
    check_output_stream(output.data, expected).await;

    let output = execute_sql(&test_context.frontend(), "show tables;").await;
    let expected = r#"+------------------+
| Tables_in_public |
+------------------+
| grpc_latencies   |
| numbers          |
+------------------+"#;
    check_output_stream(output.data, expected).await;
}

#[tokio::test]
async fn test_rename_table() {}

async fn unwrap_err(output: OutputData) -> String {
    let error = match output {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap_err(),
        _ => unreachable!(),
    };
    format!("{error:?}")
}

async fn assert_no_field_error(output: OutputData) {
    let error = unwrap_err(output).await;
    assert!(error.contains("No field named"));
}

async fn assert_has_different_type(output: OutputData) {
    let error = unwrap_err(output).await;
    assert!(error.contains("schema has a different type"));
}

async fn region_not_found(output: OutputData) {
    let error = unwrap_err(output).await;
    assert!(error.contains("not found"));
    assert!(error.contains("RegionId"));
}

async fn reconcile_table(
    procedure_manager: &ProcedureManagerRef,
    reconciliation_manager: &ReconciliationManagerRef,
    table_name: &str,
) {
    let table_ref = TableReference {
        catalog: DEFAULT_CATALOG_NAME,
        schema: DEFAULT_SCHEMA_NAME,
        table: table_name,
    };
    let procedure_id = reconciliation_manager
        .reconcile_table(table_ref, ResolveStrategy::default())
        .await
        .unwrap();
    wait_procedure(procedure_manager, procedure_id).await;
}

async fn assert_affected_rows(output: OutputData) {
    assert!(matches!(output, OutputData::AffectedRows(_)));
}
