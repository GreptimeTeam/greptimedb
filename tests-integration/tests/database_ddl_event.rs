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

use client::OutputData;
use common_test_util::temp_dir::create_temp_dir;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::{StorageType, get_test_store_config};

use crate::event_recorder_test_util::{assert_single_event, find_eventually_string};

const DATABASE_NAME: &str = "database_ddl_events";

#[tokio::test(flavor = "multi_thread")]
async fn test_database_ddl_events() {
    let store_type = StorageType::File;
    if !store_type.test_on() {
        return;
    }

    common_telemetry::init_default_ut_logging();
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_database_ddl_events_data_home");
    let cluster = GreptimeDbClusterBuilder::new("test_database_ddl_events")
        .await
        .with_datanodes(1)
        .with_store_config(store_config)
        .with_shared_home_dir(Arc::new(home_dir))
        .build(true)
        .await;
    let instance = cluster.fe_instance();

    execute_database_ddl(instance).await;

    assert_database_events(instance).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_standalone_database_ddl_events() {
    common_telemetry::init_default_ut_logging();
    let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_database_ddl_events")
        .build()
        .await;
    let instance = standalone.fe_instance();

    execute_database_ddl(instance).await;

    assert_database_events(instance).await;
}

async fn execute_database_ddl(instance: &Arc<frontend::instance::Instance>) {
    for sql in [
        format!("CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} WITH (ttl = '1h')"),
        format!("ALTER DATABASE {DATABASE_NAME} SET 'ttl' = '2h'"),
        format!("DROP DATABASE IF EXISTS {DATABASE_NAME}"),
    ] {
        let output = instance
            .do_query(&sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap();
        assert!(matches!(output.data, OutputData::AffectedRows(_)));
    }
}

async fn assert_database_events(instance: &Arc<frontend::instance::Instance>) {
    assert_database_event(
        instance,
        "create_database",
        r#"json_path_match(payload, '$.version == 1')
   AND json_path_match(payload, '$.create_if_not_exists == true')
   AND json_path_match(payload, '$.options[0].key == "ttl"')
   AND json_path_match(payload, '$.options[0].value == "1h"')"#,
    )
    .await;
    assert_database_event(
        instance,
        "alter_database",
        r#"json_path_match(payload, '$.version == 1')
   AND json_path_match(payload, '$.action == "set"')
   AND json_path_match(payload, '$.options[0].key == "ttl"')
   AND json_path_match(payload, '$.options[0].value == "2h"')"#,
    )
    .await;
    assert_database_event(
        instance,
        "drop_database",
        r#"json_path_match(payload, '$.version == 1')
   AND json_path_match(payload, '$.drop_if_exists == true')"#,
    )
    .await;
}

async fn assert_database_event(
    instance: &Arc<frontend::instance::Instance>,
    event_type: &str,
    submitted_payload_predicate: &str,
) {
    let submitted = format!(
        r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{event_type}'
  AND procedure_state = 'Running'
  AND json_path_match(procedure_trigger, '$.type == "Submitted"')
  AND catalog_name = 'greptime'
  AND schema_name = '{DATABASE_NAME}'
  AND {submitted_payload_predicate}"#
    );
    assert_single_event(instance, &submitted).await;
    let actual = find_eventually_string(
        instance,
        &format!(
            "SELECT json_to_string(trigger_context) AS trigger_context FROM greptime_private.events WHERE type = '{event_type}' AND procedure_state = 'Running' AND json_path_match(procedure_trigger, '$.type == \"Submitted\"') ORDER BY timestamp DESC LIMIT 1"
        ),
        "trigger_context",
    )
    .await;
    assert_eq!(r#"{"protocol":"unknown","reason":"manual"}"#, actual);

    let lifecycle = format!(
        r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{event_type}'
  AND procedure_state = 'Done'
  AND json_path_match(procedure_trigger, '$.type == "Succeeded"')
  AND catalog_name IS NULL
  AND schema_name IS NULL
  AND json_is_null(payload)"#
    );
    assert_single_event(instance, &lifecycle).await;
}
