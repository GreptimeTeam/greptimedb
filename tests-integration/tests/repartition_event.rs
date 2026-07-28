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

use common_recordbatch::RecordBatches;
use common_test_util::temp_dir::create_temp_dir;
use meta_srv::gc::GcSchedulerOptions;
use mito2::gc::GcConfig;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::test_util::{StorageType, get_test_store_config};

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
    assert_repartition_event(instance).await;
    assert_repartition_group_event(instance).await;
    execute_merge(instance).await;
    assert_merge_repartition_group_event(instance).await;
}

async fn execute_partition(instance: &Arc<frontend::instance::Instance>) {
    for sql in [
        format!(
            "CREATE TABLE {TABLE_NAME} (id INT, ts TIMESTAMP TIME INDEX, PRIMARY KEY(id)) ENGINE = mito"
        ),
        format!(
            "INSERT INTO {TABLE_NAME} VALUES (1, '2024-01-01 00:00:00'), (10, '2024-01-01 00:00:00')"
        ),
        format!("ALTER TABLE {TABLE_NAME} PARTITION ON COLUMNS (id) (id < 10, id >= 10)"),
    ] {
        instance
            .do_query(&sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap();
    }
}

async fn execute_merge(instance: &Arc<frontend::instance::Instance>) {
    let sql = format!("ALTER TABLE {TABLE_NAME} MERGE PARTITION (id < 10, id >= 10)");
    instance
        .do_query(&sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
}

async fn assert_repartition_event(instance: &Arc<frontend::instance::Instance>) {
    let query = format!(
        r#"SELECT type, catalog_name, schema_name, table_name, table_id,
    json_path_match(payload, '$.version == 1') AS payload_version,
    json_path_match(payload, '$.source.type == "unpartitioned"') AS unpartitioned_source,
    json_path_match(payload, '$.source.partition_exprs == []') AS empty_source_exprs,
    json_path_match(payload, '$.target_partition_columns[0] == "id"') AS target_column
FROM greptime_private.events
WHERE type = 'repartition'
  AND procedure_trigger = 'Submitted'
  AND table_name = '{TABLE_NAME}'"#
    );
    let expected = "\
+-------------+--------------+-------------+--------------------+----------+-----------------+----------------------+--------------------+---------------+
| type        | catalog_name | schema_name | table_name         | table_id | payload_version | unpartitioned_source | empty_source_exprs | target_column |
+-------------+--------------+-------------+--------------------+----------+-----------------+----------------------+--------------------+---------------+
| repartition | greptime     | public      | repartition_events | 1024     | true            | true                 | true               | true          |
+-------------+--------------+-------------+--------------------+----------+-----------------+----------------------+--------------------+---------------+";

    assert_eventually(instance, &query, expected).await;
}

async fn assert_repartition_group_event(instance: &Arc<frontend::instance::Instance>) {
    let query = format!(
        r#"SELECT type, catalog_name, schema_name, table_name, table_id,
    parent_procedure_id IS NOT NULL AS has_parent_procedure_id,
    repartition_group_id IS NOT NULL AS has_group_id,
    source_region_id,
    source_region_number,
    source_partition_expr IS NULL AS default_source,
    target_region_id,
    target_region_number,
    target_partition_expr
FROM greptime_private.events
WHERE type = 'repartition_group'
  AND procedure_trigger = 'Submitted'
  AND table_name = '{TABLE_NAME}'
ORDER BY target_region_id"#
    );
    let expected = "\
+-------------------+--------------+-------------+--------------------+----------+-------------------------+--------------+------------------+----------------------+----------------+------------------+----------------------+-----------------------+
| type              | catalog_name | schema_name | table_name         | table_id | has_parent_procedure_id | has_group_id | source_region_id | source_region_number | default_source | target_region_id | target_region_number | target_partition_expr |
+-------------------+--------------+-------------+--------------------+----------+-------------------------+--------------+------------------+----------------------+----------------+------------------+----------------------+-----------------------+
| repartition_group | greptime     | public      | repartition_events | 1024     | true                    | true         | 4398046511104    | 0                    | true           | 4398046511104    | 0                    | id < 10               |
| repartition_group | greptime     | public      | repartition_events | 1024     | true                    | true         | 4398046511104    | 0                    | true           | 4398046511105    | 1                    | id >= 10              |
+-------------------+--------------+-------------+--------------------+----------+-------------------------+--------------+------------------+----------------------+----------------+------------------+----------------------+-----------------------+";

    assert_eventually(instance, &query, expected).await;
}

async fn assert_merge_repartition_group_event(instance: &Arc<frontend::instance::Instance>) {
    let query = format!(
        r#"SELECT type, catalog_name, schema_name, table_name, table_id,
    parent_procedure_id IS NOT NULL AS has_parent_procedure_id,
    repartition_group_id IS NOT NULL AS has_group_id,
    source_region_id,
    source_region_number,
    source_partition_expr IS NOT NULL AS partitioned_source,
    target_region_id,
    target_region_number,
    target_partition_expr IS NOT NULL AS target_expr
FROM greptime_private.events
WHERE type = 'repartition_group'
  AND procedure_trigger = 'Submitted'
  AND table_name = '{TABLE_NAME}'
  AND source_partition_expr IS NOT NULL
ORDER BY source_region_id"#
    );
    let expected = "\
+-------------------+--------------+-------------+--------------------+----------+-------------------------+--------------+------------------+----------------------+--------------------+------------------+----------------------+-------------+
| type              | catalog_name | schema_name | table_name         | table_id | has_parent_procedure_id | has_group_id | source_region_id | source_region_number | partitioned_source | target_region_id | target_region_number | target_expr |
+-------------------+--------------+-------------+--------------------+----------+-------------------------+--------------+------------------+----------------------+--------------------+------------------+----------------------+-------------+
| repartition_group | greptime     | public      | repartition_events | 1024     | true                    | true         | 4398046511104    | 0                    | true               | 4398046511104    | 0                    | true        |
| repartition_group | greptime     | public      | repartition_events | 1024     | true                    | true         | 4398046511105    | 1                    | true               | 4398046511104    | 0                    | true        |
+-------------------+--------------+-------------+--------------------+----------+-------------------------+--------------+------------------+----------------------+--------------------+------------------+----------------------+-------------+";

    assert_eventually(instance, &query, expected).await;
}

async fn assert_eventually(
    instance: &Arc<frontend::instance::Instance>,
    query: &str,
    expected: &str,
) {
    for _ in 0..120 {
        if event_output(instance, query).await.as_deref() == Some(expected) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    panic!("timed out waiting for repartition event: {query}");
}

async fn event_output(instance: &Arc<frontend::instance::Instance>, query: &str) -> Option<String> {
    let output = instance
        .do_query(query, QueryContext::arc())
        .await
        .remove(0)
        .ok()?;
    let client::OutputData::Stream(stream) = output.data else {
        return None;
    };
    let batches = RecordBatches::try_collect(stream).await.ok()?;
    batches.pretty_print().ok()
}
