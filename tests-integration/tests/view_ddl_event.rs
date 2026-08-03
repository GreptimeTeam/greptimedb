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

use common_test_util::temp_dir::create_temp_dir;
use common_wal::config::DatanodeWalConfig;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::{StorageType, get_test_store_config};
use uuid::Uuid;

use crate::event_recorder_test_util::{assert_single_event, find_eventually_string};

const CREATE_VIEW_EVENT_TYPE: &str = "create_view";
const DROP_VIEW_EVENT_TYPE: &str = "drop_view";

#[tokio::test(flavor = "multi_thread")]
async fn test_view_ddl_events() {
    let store_type = StorageType::File;
    if !store_type.test_on() {
        return;
    }

    common_telemetry::init_default_ut_logging();
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_view_ddl_events_data_home");
    let cluster = GreptimeDbClusterBuilder::new("test_view_ddl_events")
        .await
        .with_datanodes(1)
        .with_store_config(store_config)
        .with_datanode_wal_config(DatanodeWalConfig::Noop)
        .with_shared_home_dir(Arc::new(home_dir))
        .build(true)
        .await;
    let instance = cluster.fe_instance();
    let suffix = Uuid::new_v4().simple();
    let source_table = format!("view_ddl_event_source_{suffix}");
    let view = format!("view_ddl_event_{suffix}");

    execute_view_ddl(instance, &source_table, &view).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_standalone_view_ddl_events() {
    common_telemetry::init_default_ut_logging();
    let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_view_ddl_events")
        .build()
        .await;
    let suffix = Uuid::new_v4().simple();
    let source_table = format!("view_ddl_event_source_{suffix}");
    let view = format!("view_ddl_event_{suffix}");

    execute_view_ddl(standalone.fe_instance(), &source_table, &view).await;
}

async fn execute_view_ddl(
    instance: &Arc<frontend::instance::Instance>,
    source_table: &str,
    view: &str,
) {
    instance
        .do_query(
            &format!(
                "CREATE TABLE {source_table} (host STRING PRIMARY KEY, amount DOUBLE, ts TIMESTAMP TIME INDEX)"
            ),
            QueryContext::arc(),
        )
        .await
        .remove(0)
        .unwrap();

    instance
        .do_query(
            &format!("CREATE VIEW {view} AS SELECT amount FROM {source_table}"),
            QueryContext::arc(),
        )
        .await
        .remove(0)
        .unwrap();
    assert_create_events(instance, view).await;

    instance
        .do_query(&format!("DROP VIEW {view}"), QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    assert_drop_events(instance, view).await;
}

async fn assert_create_events(instance: &Arc<frontend::instance::Instance>, view: &str) {
    let procedure_id = find_submitted_procedure_id(instance, CREATE_VIEW_EVENT_TYPE, view).await;
    assert_single_event(
        instance,
        &format!(
            r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{CREATE_VIEW_EVENT_TYPE}'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Running'
  AND json_path_match(procedure_trigger, '$.type == "Submitted"')
  AND catalog_name = 'greptime'
  AND schema_name = 'public'
  AND view_name = '{view}'
  AND view_id IS NULL
  AND json_path_match(payload, '$.version == 1')
  AND json_path_match(payload, '$.or_replace == false')
  AND json_path_match(payload, '$.create_if_not_exists == false')
  AND json_path_match(payload, '$.referenced_table_count == 1')
  AND json_path_match(payload, '$.column_count == 0')"#,
        ),
    )
    .await;
    assert_single_event(
        instance,
        &format!(
            r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{CREATE_VIEW_EVENT_TYPE}'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Done'
  AND json_path_match(procedure_trigger, '$.type == "Succeeded"')
  AND catalog_name IS NULL
  AND schema_name IS NULL
  AND view_name IS NULL
  AND view_id IS NOT NULL
  AND json_is_null(payload)"#,
        ),
    )
    .await;
}

async fn assert_drop_events(instance: &Arc<frontend::instance::Instance>, view: &str) {
    let procedure_id = find_submitted_procedure_id(instance, DROP_VIEW_EVENT_TYPE, view).await;
    assert_single_event(
        instance,
        &format!(
            r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{DROP_VIEW_EVENT_TYPE}'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Running'
  AND json_path_match(procedure_trigger, '$.type == "Submitted"')
  AND catalog_name = 'greptime'
  AND schema_name = 'public'
  AND view_name = '{view}'
  AND view_id IS NOT NULL
  AND json_path_match(payload, '$.version == 1')
  AND json_path_match(payload, '$.drop_if_exists == false')"#,
        ),
    )
    .await;
    assert_single_event(
        instance,
        &format!(
            r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{DROP_VIEW_EVENT_TYPE}'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Done'
  AND json_path_match(procedure_trigger, '$.type == "Succeeded"')
  AND catalog_name IS NULL
  AND schema_name IS NULL
  AND view_name IS NULL
  AND view_id IS NULL
  AND json_is_null(payload)"#,
        ),
    )
    .await;
}

async fn find_submitted_procedure_id(
    instance: &Arc<frontend::instance::Instance>,
    event_type: &str,
    view_name: &str,
) -> String {
    find_eventually_string(
        instance,
        &format!(
            r#"SELECT procedure_id
FROM greptime_private.events
WHERE type = '{event_type}'
  AND view_name = '{view_name}'
  AND json_path_match(procedure_trigger, '$.type == "Submitted"')
ORDER BY timestamp DESC
LIMIT 1"#,
        ),
        "procedure_id",
    )
    .await
}
