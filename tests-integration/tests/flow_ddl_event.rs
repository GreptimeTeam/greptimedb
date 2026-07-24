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
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::test_util::{StorageType, get_test_store_config};
use uuid::Uuid;

use crate::event_recorder_test_util::{assert_single_event, find_eventually_string};

const CREATE_FLOW_EVENT_TYPE: &str = "create_flow";
const DROP_FLOW_EVENT_TYPE: &str = "drop_flow";

#[tokio::test(flavor = "multi_thread")]
async fn test_flow_ddl_events_file() {
    let store_type = StorageType::File;
    if !store_type.test_on() {
        return;
    }

    common_telemetry::init_default_ut_logging();
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_flow_ddl_events_data_home");
    let cluster = GreptimeDbClusterBuilder::new("test_flow_ddl_events")
        .await
        .with_datanodes(1)
        .with_store_config(store_config)
        .with_shared_home_dir(Arc::new(home_dir))
        .build(true)
        .await;
    let instance = cluster.fe_instance();
    let suffix = Uuid::new_v4().simple();
    let missing_source = format!("flow_ddl_event_missing_source_{suffix}");
    let sink = format!("flow_ddl_event_sink_{suffix}");
    let flow = format!("flow_ddl_event_{suffix}");

    instance
        .do_query(
            &format!("CREATE TABLE {sink} (val STRING, ts TIMESTAMP TIME INDEX)"),
            QueryContext::arc(),
        )
        .await
        .remove(0)
        .unwrap();

    instance
        .do_query(
            &format!(
                "CREATE FLOW IF NOT EXISTS {flow} SINK TO {sink} EVAL INTERVAL '10s' \\
                 WITH (defer_on_missing_source = true) AS SELECT val, ts FROM {missing_source}"
            ),
            QueryContext::arc(),
        )
        .await
        .remove(0)
        .unwrap();
    assert_create_events(instance, &flow).await;

    instance
        .do_query(&format!("DROP FLOW IF EXISTS {flow}"), QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    assert_drop_events(instance, &flow).await;
}

async fn assert_create_events(instance: &Arc<frontend::instance::Instance>, flow: &str) {
    let procedure_id = find_submitted_procedure_id(instance, CREATE_FLOW_EVENT_TYPE, flow).await;
    assert_single_event(
        instance,
        &format!(
            r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{CREATE_FLOW_EVENT_TYPE}'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Running'
  AND procedure_trigger = 'Submitted'
  AND catalog_name = 'greptime'
  AND schema_name = 'public'
  AND flow_name = '{flow}'
  AND flow_id IS NULL
  AND json_path_match(payload, '$.version == 1')
  AND json_path_match(payload, '$.or_replace == false')
  AND json_path_match(payload, '$.create_if_not_exists == true')
  AND json_path_match(payload, '$.expire_after == null')
  AND json_path_match(payload, '$.eval_interval_secs == 10')"#,
        ),
    )
    .await;
    assert_single_event(
        instance,
        &format!(
            r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{CREATE_FLOW_EVENT_TYPE}'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Done'
  AND procedure_trigger = 'Succeeded'
  AND catalog_name IS NULL
  AND schema_name IS NULL
  AND flow_name IS NULL
  AND flow_id IS NOT NULL
  AND json_is_null(payload)"#,
        ),
    )
    .await;
}

async fn assert_drop_events(instance: &Arc<frontend::instance::Instance>, flow: &str) {
    let procedure_id = find_submitted_procedure_id(instance, DROP_FLOW_EVENT_TYPE, flow).await;
    assert_single_event(
        instance,
        &format!(
            r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{DROP_FLOW_EVENT_TYPE}'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Running'
  AND procedure_trigger = 'Submitted'
  AND catalog_name = 'greptime'
  AND schema_name IS NULL
  AND flow_name = '{flow}'
  AND flow_id IS NOT NULL
  AND json_path_match(payload, '$.version == 1')
  AND json_path_match(payload, '$.drop_if_exists == true')"#,
        ),
    )
    .await;
    assert_single_event(
        instance,
        &format!(
            r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{DROP_FLOW_EVENT_TYPE}'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Done'
  AND procedure_trigger = 'Succeeded'
  AND catalog_name IS NULL
  AND schema_name IS NULL
  AND flow_name IS NULL
  AND flow_id IS NULL
  AND json_is_null(payload)"#,
        ),
    )
    .await;
}

async fn find_submitted_procedure_id(
    instance: &Arc<frontend::instance::Instance>,
    event_type: &str,
    flow_name: &str,
) -> String {
    find_eventually_string(
        instance,
        &format!(
            "SELECT procedure_id FROM greptime_private.events \\
             WHERE type = '{event_type}' AND flow_name = '{flow_name}' \\
             AND procedure_trigger = 'Submitted' ORDER BY timestamp DESC LIMIT 1"
        ),
        "procedure_id",
    )
    .await
}
