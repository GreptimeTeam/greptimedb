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

use common_test_util::temp_dir::create_temp_dir;
use meta_srv::gc::GcSchedulerOptions;
use mito2::gc::GcConfig;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::test_util::{StorageType, get_test_store_config};

use crate::event_recorder_test_util::{assert_eventually_eq, find_eventually_string};

const TABLE_NAME: &str = "repartition_events";

#[tokio::test(flavor = "multi_thread")]
async fn test_repartition_event() {
    let store_type = StorageType::File;
    if !store_type.test_on() {
        return;
    }

    common_telemetry::init_default_ut_logging();
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_repartition_event_data_home");
    let cluster = GreptimeDbClusterBuilder::new("test_repartition_event")
        .await
        .with_datanodes(1)
        .with_store_config(store_config)
        .with_shared_home_dir(Arc::new(home_dir))
        .with_metasrv_gc_config(GcSchedulerOptions {
            enable: true,
            gc_cooldown_period: Duration::from_nanos(1),
            ..Default::default()
        })
        .with_datanode_gc_config(GcConfig {
            enable: true,
            lingering_time: Some(Duration::ZERO),
            unknown_file_lingering_time: Duration::ZERO,
            ..Default::default()
        })
        .build(true)
        .await;
    let instance = cluster.fe_instance();

    execute_partition(instance).await;
    let partition_procedure_id = assert_repartition_event(instance).await;
    assert_repartition_group_event(instance, &partition_procedure_id).await;
    execute_merge(instance).await;
    let merge_procedure_id = find_submitted_procedure_id(
        instance,
        "repartition",
        &format!(
            "table_name = '{TABLE_NAME}' \
             AND json_path_match(payload, '$.source_type == \"partitioned\"')"
        ),
    )
    .await;
    assert_merge_repartition_group_event(instance, &merge_procedure_id).await;
}

async fn execute_partition(instance: &Arc<frontend::instance::Instance>) {
    for sql in [
        format!(
            "CREATE TABLE {TABLE_NAME} (host INT, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host)) ENGINE = mito"
        ),
        format!(
            "INSERT INTO {TABLE_NAME} VALUES (1, '2024-01-01 00:00:00'), (10, '2024-01-01 00:00:00')"
        ),
        format!("ALTER TABLE {TABLE_NAME} PARTITION ON COLUMNS (host) (host < 10, host >= 10)"),
    ] {
        instance
            .do_query(&sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap();
    }
}

async fn execute_merge(instance: &Arc<frontend::instance::Instance>) {
    let sql = format!("ALTER TABLE {TABLE_NAME} MERGE PARTITION (host < 10, host >= 10)");
    instance
        .do_query(&sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
}

async fn assert_repartition_event(instance: &Arc<frontend::instance::Instance>) -> String {
    let procedure_id = find_submitted_procedure_id(
        instance,
        "repartition",
        &format!(
            "table_name = '{TABLE_NAME}' \
             AND json_path_match(payload, '$.source_type == \"unpartitioned\"')"
        ),
    )
    .await;
    let query = format!(
        r#"SELECT type, catalog_name, schema_name, table_name, table_id,
    json_path_match(payload, '$.version == 2') AS payload_version,
    json_path_match(payload, '$.source_type == "unpartitioned"') AS unpartitioned_source,
    json_path_match(payload, '$.target_partition_columns[0] == "host"') AS target_column
FROM greptime_private.events
WHERE type = 'repartition'
  AND procedure_id = '{procedure_id}'
  AND json_path_match(procedure_trigger, '$.type == "Submitted"')
  AND table_name = '{TABLE_NAME}'"#
    );
    let expected = "\
+-------------+--------------+-------------+--------------------+----------+-----------------+----------------------+---------------+
| type        | catalog_name | schema_name | table_name         | table_id | payload_version | unpartitioned_source | target_column |
+-------------+--------------+-------------+--------------------+----------+-----------------+----------------------+---------------+
| repartition | greptime     | public      | repartition_events | 1024     | true            | true                 | true          |
+-------------+--------------+-------------+--------------------+----------+-----------------+----------------------+---------------+";

    assert_eventually_eq(instance, &query, expected).await;
    assert_repartition_lifecycle_event(instance, &procedure_id).await;
    procedure_id
}

async fn assert_repartition_lifecycle_event(
    instance: &Arc<frontend::instance::Instance>,
    procedure_id: &str,
) {
    let query = format!(
        r#"SELECT type, catalog_name, schema_name, table_name, table_id
FROM greptime_private.events
WHERE type = 'repartition'
  AND procedure_id = '{procedure_id}'
  AND json_path_match(procedure_trigger, '$.type == "Succeeded"')
  AND json_is_null(payload)"#
    );
    let expected = "\
+-------------+--------------+-------------+--------------------+----------+
| type        | catalog_name | schema_name | table_name         | table_id |
+-------------+--------------+-------------+--------------------+----------+
| repartition | greptime     | public      | repartition_events | 1024     |
+-------------+--------------+-------------+--------------------+----------+";

    assert_eventually_eq(instance, &query, expected).await;
}

async fn assert_repartition_group_event(
    instance: &Arc<frontend::instance::Instance>,
    parent_procedure_id: &str,
) {
    let procedure_id = find_submitted_procedure_id(
        instance,
        "repartition_group",
        &format!(
            "table_name = '{TABLE_NAME}' \
             AND source_partition_expr IS NULL"
        ),
    )
    .await;
    let query = format!(
        r#"SELECT type, catalog_name, schema_name, table_name, table_id,
    parent_procedure_id = '{parent_procedure_id}' AS matches_parent_procedure_id,
    repartition_group_id IS NOT NULL AS has_group_id,
    source_region_id,
    source_region_number,
    source_partition_expr IS NULL AS default_source,
    target_region_id,
    target_region_number,
    target_partition_expr
FROM greptime_private.events
WHERE type = 'repartition_group'
  AND procedure_id = '{procedure_id}'
  AND json_path_match(procedure_trigger, '$.type == "Submitted"')
  AND table_name = '{TABLE_NAME}'
ORDER BY target_region_id"#
    );
    let expected = "\
+-------------------+--------------+-------------+--------------------+----------+-----------------------------+--------------+------------------+----------------------+----------------+------------------+----------------------+-----------------------+
| type              | catalog_name | schema_name | table_name         | table_id | matches_parent_procedure_id | has_group_id | source_region_id | source_region_number | default_source | target_region_id | target_region_number | target_partition_expr |
+-------------------+--------------+-------------+--------------------+----------+-----------------------------+--------------+------------------+----------------------+----------------+------------------+----------------------+-----------------------+
| repartition_group | greptime     | public      | repartition_events | 1024     | true                        | true         | 4398046511104    | 0                    | true           | 4398046511104    | 0                    | host < 10             |
| repartition_group | greptime     | public      | repartition_events | 1024     | true                        | true         | 4398046511104    | 0                    | true           | 4398046511105    | 1                    | host >= 10            |
+-------------------+--------------+-------------+--------------------+----------+-----------------------------+--------------+------------------+----------------------+----------------+------------------+----------------------+-----------------------+";

    assert_eventually_eq(instance, &query, expected).await;
    assert_repartition_group_lifecycle_event(instance, &procedure_id, parent_procedure_id).await;
}

async fn assert_repartition_group_lifecycle_event(
    instance: &Arc<frontend::instance::Instance>,
    procedure_id: &str,
    parent_procedure_id: &str,
) {
    let query = format!(
        r#"SELECT type, catalog_name, schema_name, table_name, table_id,
    parent_procedure_id = '{parent_procedure_id}' AS matches_parent_procedure_id,
    repartition_group_id IS NOT NULL AS has_group_id
FROM greptime_private.events
WHERE type = 'repartition_group'
  AND procedure_id = '{procedure_id}'
  AND json_path_match(procedure_trigger, '$.type == "Succeeded"')
  AND json_is_null(payload)"#
    );
    let expected = "\
+-------------------+--------------+-------------+--------------------+----------+-----------------------------+--------------+
| type              | catalog_name | schema_name | table_name         | table_id | matches_parent_procedure_id | has_group_id |
+-------------------+--------------+-------------+--------------------+----------+-----------------------------+--------------+
| repartition_group | greptime     | public      | repartition_events | 1024     | true                        | true         |
+-------------------+--------------+-------------+--------------------+----------+-----------------------------+--------------+";

    assert_eventually_eq(instance, &query, expected).await;
}

async fn assert_merge_repartition_group_event(
    instance: &Arc<frontend::instance::Instance>,
    parent_procedure_id: &str,
) {
    let procedure_id = find_submitted_procedure_id(
        instance,
        "repartition_group",
        &format!(
            "table_name = '{TABLE_NAME}' \
             AND source_partition_expr IS NOT NULL"
        ),
    )
    .await;
    let query = format!(
        r#"SELECT type, catalog_name, schema_name, table_name, table_id,
    parent_procedure_id = '{parent_procedure_id}' AS matches_parent_procedure_id,
    repartition_group_id IS NOT NULL AS has_group_id,
    source_region_id,
    source_region_number,
    source_partition_expr IS NOT NULL AS partitioned_source,
    target_region_id,
    target_region_number,
    target_partition_expr IS NOT NULL AS target_expr
FROM greptime_private.events
WHERE type = 'repartition_group'
  AND procedure_id = '{procedure_id}'
  AND json_path_match(procedure_trigger, '$.type == "Submitted"')
  AND table_name = '{TABLE_NAME}'
  AND source_partition_expr IS NOT NULL
ORDER BY source_region_id"#
    );
    let expected = "\
+-------------------+--------------+-------------+--------------------+----------+-----------------------------+--------------+------------------+----------------------+--------------------+------------------+----------------------+-------------+
| type              | catalog_name | schema_name | table_name         | table_id | matches_parent_procedure_id | has_group_id | source_region_id | source_region_number | partitioned_source | target_region_id | target_region_number | target_expr |
+-------------------+--------------+-------------+--------------------+----------+-----------------------------+--------------+------------------+----------------------+--------------------+------------------+----------------------+-------------+
| repartition_group | greptime     | public      | repartition_events | 1024     | true                        | true         | 4398046511104    | 0                    | true               | 4398046511104    | 0                    | true        |
| repartition_group | greptime     | public      | repartition_events | 1024     | true                        | true         | 4398046511105    | 1                    | true               | 4398046511104    | 0                    | true        |
+-------------------+--------------+-------------+--------------------+----------+-----------------------------+--------------+------------------+----------------------+--------------------+------------------+----------------------+-------------+";

    assert_eventually_eq(instance, &query, expected).await;
}

async fn find_submitted_procedure_id(
    instance: &Arc<frontend::instance::Instance>,
    event_type: &str,
    predicate: &str,
) -> String {
    find_eventually_string(
        instance,
        &format!(
            r#"SELECT procedure_id
FROM greptime_private.events
WHERE type = '{event_type}'
  AND json_path_match(procedure_trigger, '$.type == "Submitted"')
  AND {predicate}
ORDER BY timestamp DESC
LIMIT 1"#
        ),
        "procedure_id",
    )
    .await
}
