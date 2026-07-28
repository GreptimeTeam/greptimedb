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

use crate::event_recorder_test_util::assert_single_event;

const CREATE_VIEW_EVENT_TYPE: &str = "create_view";
const DROP_VIEW_EVENT_TYPE: &str = "drop_view";
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
        .with_datanode_wal_config(DatanodeWalConfig::Noop)
        .with_shared_home_dir(Arc::new(home_dir))
        .build(true)
        .await;
    let instance = cluster.fe_instance();

    execute_view_ddl(instance).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_standalone_view_procedure_events() {
    common_telemetry::init_default_ut_logging();
    let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_view_procedure_events")
        .build()
        .await;

    execute_view_ddl(standalone.fe_instance()).await;
}

async fn execute_view_ddl(instance: &Arc<frontend::instance::Instance>) {
    instance
        .do_query(
            &format!(
                "CREATE TABLE {SOURCE_TABLE_NAME} (host STRING PRIMARY KEY, amount DOUBLE, ts TIMESTAMP TIME INDEX)"
            ),
            QueryContext::arc(),
        )
        .await
        .remove(0)
        .unwrap();

    instance
        .do_query(
            &format!("CREATE VIEW {VIEW_NAME} AS SELECT amount FROM {SOURCE_TABLE_NAME}"),
            QueryContext::arc(),
        )
        .await
        .remove(0)
        .unwrap();
    assert_create_events(instance).await;

    instance
        .do_query(&format!("DROP VIEW {VIEW_NAME}"), QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    assert_drop_events(instance).await;
}

async fn assert_create_events(instance: &Arc<frontend::instance::Instance>) {
    assert_single_event(
        instance,
        &format!(
            r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{CREATE_VIEW_EVENT_TYPE}'
  AND procedure_state = 'Running'
  AND procedure_trigger = 'Submitted'
  AND catalog_name = 'greptime'
  AND schema_name = 'public'
  AND view_name = '{VIEW_NAME}'
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
  AND procedure_state = 'Done'
  AND procedure_trigger = 'Succeeded'
  AND catalog_name IS NULL
  AND schema_name IS NULL
  AND view_name IS NULL
  AND view_id IS NOT NULL
  AND json_is_null(payload)"#,
        ),
    )
    .await;
}

async fn assert_drop_events(instance: &Arc<frontend::instance::Instance>) {
    assert_single_event(
        instance,
        &format!(
            r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{DROP_VIEW_EVENT_TYPE}'
  AND procedure_state = 'Running'
  AND procedure_trigger = 'Submitted'
  AND catalog_name = 'greptime'
  AND schema_name = 'public'
  AND view_name = '{VIEW_NAME}'
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
  AND procedure_state = 'Done'
  AND procedure_trigger = 'Succeeded'
  AND catalog_name IS NULL
  AND schema_name IS NULL
  AND view_name IS NULL
  AND view_id IS NULL
  AND json_is_null(payload)"#,
        ),
    )
    .await;
}
