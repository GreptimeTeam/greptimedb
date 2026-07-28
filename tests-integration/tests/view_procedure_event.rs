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
use common_recordbatch::RecordBatches;
use common_test_util::temp_dir::create_temp_dir;
use datatypes::arrow::array::{Array, UInt32Array};
use frontend::instance::Instance;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::{StorageType, get_test_store_config};

const VIEW_NAME: &str = "ddl_view_procedure_event_view";
const SOURCE_TABLE_NAME: &str = "ddl_view_procedure_event_source";

#[tokio::test(flavor = "multi_thread")]
async fn test_view_procedure_events() {
    let store_type = StorageType::File;
    if !store_type.test_on() {
        return;
    }

    common_telemetry::init_default_ut_logging();
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_view_procedure_events_data_home");
    let cluster = GreptimeDbClusterBuilder::new("test_view_procedure_events")
        .await
        .with_datanodes(1)
        .with_store_config(store_config)
        .with_shared_home_dir(Arc::new(home_dir))
        .build(true)
        .await;
    let instance = cluster.fe_instance();

    execute_view_ddl(instance).await;
    assert_view_events(instance).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_standalone_view_procedure_events() {
    common_telemetry::init_default_ut_logging();
    let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_view_procedure_events")
        .build()
        .await;
    let instance = standalone.fe_instance();

    execute_view_ddl(instance).await;
    assert_view_events(instance).await;
}

async fn execute_view_ddl(instance: &Arc<Instance>) {
    for sql in [
        format!(
            "CREATE TABLE {SOURCE_TABLE_NAME} (host STRING PRIMARY KEY, amount DOUBLE, ts TIMESTAMP TIME INDEX)"
        ),
        format!("CREATE VIEW {VIEW_NAME} AS SELECT amount FROM {SOURCE_TABLE_NAME}"),
        format!("DROP VIEW {VIEW_NAME}"),
    ] {
        let output = instance
            .do_query(&sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap();
        assert!(matches!(output.data, OutputData::AffectedRows(_)));
    }
}

async fn assert_view_events(instance: &Arc<Instance>) {
    assert_view_event(
        instance,
        "create_view",
        &format!(
            r#"view_name = '{VIEW_NAME}'
  AND view_id IS NULL
  AND json_path_match(payload, '$.version == 1')
  AND json_path_match(payload, '$.or_replace == false')
  AND json_path_match(payload, '$.create_if_not_exists == false')
  AND json_path_match(payload, '$.referenced_table_count == 1')
  AND json_path_match(payload, '$.column_count == 0')"#
        ),
    )
    .await;

    let view_id = create_view_id(instance).await;
    assert_view_lifecycle_event(instance, "create_view", Some(view_id)).await;

    assert_view_event(
        instance,
        "drop_view",
        &format!(
            r#"view_name = '{VIEW_NAME}'
  AND view_id = {view_id}
  AND json_path_match(payload, '$.version == 1')
  AND json_path_match(payload, '$.drop_if_exists == false')"#
        ),
    )
    .await;
    assert_view_lifecycle_event(instance, "drop_view", None).await;
}

async fn assert_view_event(instance: &Arc<Instance>, event_type: &str, submitted_predicate: &str) {
    let submitted = format!(
        r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{event_type}'
  AND procedure_state = 'Running'
  AND procedure_trigger = 'Submitted'
  AND catalog_name = 'greptime'
  AND schema_name = 'public'
  AND {submitted_predicate}"#
    );
    assert_single_event(instance, &submitted).await;
}

async fn assert_view_lifecycle_event(
    instance: &Arc<Instance>,
    event_type: &str,
    view_id: Option<u32>,
) {
    let view_id_predicate = match view_id {
        Some(view_id) => format!("view_id = {view_id}"),
        None => "view_id IS NULL".to_string(),
    };
    let lifecycle = format!(
        r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{event_type}'
  AND procedure_state = 'Done'
  AND procedure_trigger = 'Succeeded'
  AND catalog_name IS NULL
  AND schema_name IS NULL
  AND view_name IS NULL
  AND {view_id_predicate}
  AND json_is_null(payload)"#
    );
    assert_single_event(instance, &lifecycle).await;
}

async fn create_view_id(instance: &Arc<Instance>) -> u32 {
    let query = r#"SELECT view_id
FROM greptime_private.events
WHERE type = 'create_view'
  AND procedure_state = 'Done'
  AND procedure_trigger = 'Succeeded'
  AND view_id IS NOT NULL
  AND json_is_null(payload)"#;

    for _ in 0..60 {
        if let Some(view_id) = event_view_id(instance, query).await {
            return view_id;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    panic!("timed out waiting for create-view id: {query}");
}

async fn assert_single_event(instance: &Arc<Instance>, query: &str) {
    for _ in 0..60 {
        if event_count_is_one(instance, query).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    panic!("timed out waiting for view DDL event: {query}");
}

async fn event_count_is_one(instance: &Arc<Instance>, query: &str) -> bool {
    let Ok(output) = instance
        .do_query(query, QueryContext::arc())
        .await
        .remove(0)
    else {
        return false;
    };
    let OutputData::Stream(stream) = output.data else {
        unreachable!("event-count query must return a stream");
    };
    let Ok(batches) = RecordBatches::try_collect(stream).await else {
        return false;
    };
    let Ok(actual) = batches.pretty_print() else {
        return false;
    };

    actual
        == "\\
+-------------+
| event_count |
+-------------+
| 1           |
+-------------+"
}

async fn event_view_id(instance: &Arc<Instance>, query: &str) -> Option<u32> {
    let output = instance
        .do_query(query, QueryContext::arc())
        .await
        .remove(0)
        .ok()?;
    let OutputData::Stream(stream) = output.data else {
        unreachable!("view-id query must return a stream");
    };
    let batches = RecordBatches::try_collect(stream).await.ok()?;
    let batch = batches.iter().next()?;
    let view_ids = batch
        .column_by_name("view_id")?
        .as_any()
        .downcast_ref::<UInt32Array>()?;

    (!view_ids.is_null(0)).then(|| view_ids.value(0))
}
