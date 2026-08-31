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

use client::{Database, OutputData};
use common_event_recorder::{EventRecorderOptions, EventTypeFilter};
use common_test_util::temp_dir::create_temp_dir;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::{
    StorageType, execute_sql, get_test_store_config, setup_authenticated_grpc_database,
};

use crate::event_recorder_test_util::{
    assert_eventually_eq, assert_procedure_actor, assert_single_event, find_eventually_string,
};

const DATABASE_NAME: &str = "database_ddl_events";
const PROCEDURE_ACTOR: &str = "procedure_actor";
const PROCEDURE_ACTOR_PASSWORD: &str = "procedure_actor_pwd";
const CREATION_DATABASE_NAME: &str = "event_schema_creation";
const RECONCILIATION_DATABASE_NAME: &str = "event_schema_reconciliation";

#[tokio::test(flavor = "multi_thread")]
async fn test_event_table_auto_creation_with_auto_create_disabled() {
    common_telemetry::init_default_ut_logging();
    let standalone = GreptimeDbStandaloneBuilder::new("test_event_table_auto_creation")
        .with_auto_create_table(false)
        .with_event_recorder_options(EventRecorderOptions {
            event_types: Arc::new(EventTypeFilter::Only(
                [String::from("create_database")].into_iter().collect(),
            )),
            ..Default::default()
        })
        .build()
        .await;
    let instance = standalone.fe_instance();

    let (database, _grpc_server) = setup_authenticated_grpc_database(
        instance.clone(),
        PROCEDURE_ACTOR,
        PROCEDURE_ACTOR_PASSWORD,
    )
    .await;
    database
        .sql(format!("CREATE DATABASE {CREATION_DATABASE_NAME}"))
        .await
        .unwrap();

    assert_eventually_eq(
        instance,
        "SELECT count(*) AS events_tables \
         FROM information_schema.tables \
         WHERE table_catalog = 'greptime' \
           AND table_schema = 'greptime_private' \
           AND table_name = 'events'",
        "+---------------+\n| events_tables |\n+---------------+\n| 1             |\n+---------------+",
    )
    .await;

    let procedure_id = find_eventually_string(
        instance,
        &format!(
            "SELECT procedure_id FROM greptime_private.events \
             WHERE type = 'create_database' \
               AND schema_name = '{CREATION_DATABASE_NAME}' \
               AND json_path_match(procedure_trigger, '$.type == \"Submitted\"') \
             ORDER BY timestamp DESC LIMIT 1"
        ),
        "procedure_id",
    )
    .await;
    assert_procedure_actor(instance, &procedure_id, Some(PROCEDURE_ACTOR)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_event_table_schema_reconciliation_with_auto_create_disabled() {
    common_telemetry::init_default_ut_logging();
    let standalone = GreptimeDbStandaloneBuilder::new("test_event_table_schema_reconciliation")
        .with_auto_create_table(false)
        .with_event_recorder_options(EventRecorderOptions {
            event_types: Arc::new(EventTypeFilter::Only(
                [String::from("create_database")].into_iter().collect(),
            )),
            ..Default::default()
        })
        .build()
        .await;
    let instance = standalone.fe_instance();

    // Matches the pre-actor procedure-event schema. The recorder filter keeps
    // this setup DDL from creating an event before the regression is exercised.
    execute_sql(
        instance,
        r#"
            CREATE TABLE greptime_private.events (
                "type" STRING,
                payload JSON,
                "timestamp" TIMESTAMP(9) NOT NULL,
                procedure_id STRING,
                procedure_state STRING,
                procedure_error STRING,
                procedure_trigger JSON,
                catalog_name STRING,
                schema_name STRING,
                event_context JSON,
                TIME INDEX ("timestamp"),
                PRIMARY KEY ("type")
            ) WITH (append_mode = 'true')
        "#,
    )
    .await;

    assert_eventually_eq(
        instance,
        "SELECT count(*) AS actor_columns \
         FROM information_schema.columns \
         WHERE table_catalog = 'greptime' \
           AND table_schema = 'greptime_private' \
           AND table_name = 'events' \
           AND column_name = 'actor'",
        "+---------------+\n| actor_columns |\n+---------------+\n| 0             |\n+---------------+",
    )
    .await;

    let (database, _grpc_server) = setup_authenticated_grpc_database(
        instance.clone(),
        PROCEDURE_ACTOR,
        PROCEDURE_ACTOR_PASSWORD,
    )
    .await;
    database
        .sql(format!("CREATE DATABASE {RECONCILIATION_DATABASE_NAME}"))
        .await
        .unwrap();

    assert_eventually_eq(
        instance,
        "SELECT count(*) AS actor_columns \
         FROM information_schema.columns \
         WHERE table_catalog = 'greptime' \
           AND table_schema = 'greptime_private' \
           AND table_name = 'events' \
           AND column_name = 'actor'",
        "+---------------+\n| actor_columns |\n+---------------+\n| 1             |\n+---------------+",
    )
    .await;

    let procedure_id = find_eventually_string(
        instance,
        &format!(
            "SELECT procedure_id FROM greptime_private.events \
             WHERE type = 'create_database' \
               AND schema_name = '{RECONCILIATION_DATABASE_NAME}' \
               AND json_path_match(procedure_trigger, '$.type == \"Submitted\"') \
             ORDER BY timestamp DESC LIMIT 1"
        ),
        "procedure_id",
    )
    .await;
    assert_procedure_actor(instance, &procedure_id, Some(PROCEDURE_ACTOR)).await;
}

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
        .with_frontend_auto_create_table(false)
        .with_store_config(store_config)
        .with_shared_home_dir(Arc::new(home_dir))
        .build(true)
        .await;
    let instance = cluster.fe_instance();
    let (database, _grpc_server) = setup_authenticated_grpc_database(
        instance.clone(),
        PROCEDURE_ACTOR,
        PROCEDURE_ACTOR_PASSWORD,
    )
    .await;

    execute_database_ddl(&database).await;

    assert_database_events(instance).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_standalone_database_ddl_events() {
    common_telemetry::init_default_ut_logging();
    let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_database_ddl_events")
        .build()
        .await;
    let instance = standalone.fe_instance();
    let (database, _grpc_server) = setup_authenticated_grpc_database(
        instance.clone(),
        PROCEDURE_ACTOR,
        PROCEDURE_ACTOR_PASSWORD,
    )
    .await;

    execute_database_ddl(&database).await;

    assert_database_events(instance).await;
}

async fn execute_database_ddl(database: &Database) {
    for sql in [
        format!("CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} WITH (ttl = '1h')"),
        format!("ALTER DATABASE {DATABASE_NAME} SET 'ttl' = '2h'"),
        format!("DROP DATABASE IF EXISTS {DATABASE_NAME}"),
    ] {
        let output = database.sql(sql).await.unwrap();
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
    let procedure_id = find_eventually_string(
        instance,
        &format!(
            r#"SELECT procedure_id
FROM greptime_private.events
WHERE type = '{event_type}'
  AND schema_name = '{DATABASE_NAME}'
  AND json_path_match(procedure_trigger, '$.type == "Submitted"')
ORDER BY timestamp DESC
LIMIT 1"#
        ),
        "procedure_id",
    )
    .await;
    assert_procedure_actor(instance, &procedure_id, Some(PROCEDURE_ACTOR)).await;

    let submitted = format!(
        r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{event_type}'
  AND procedure_id = '{procedure_id}'
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
            "SELECT json_to_string(event_context) AS event_context FROM greptime_private.events WHERE procedure_id = '{procedure_id}' AND json_path_match(procedure_trigger, '$.type == \"Submitted\"')"
        ),
        "event_context",
    )
    .await;
    assert_eq!(r#"{"protocol":"grpc","reason":"manual"}"#, actual);

    let lifecycle = format!(
        r#"SELECT count(*) AS event_count
FROM greptime_private.events
WHERE type = '{event_type}'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Done'
  AND json_path_match(procedure_trigger, '$.type == "Succeeded"')
  AND catalog_name = 'greptime'
  AND schema_name = '{DATABASE_NAME}'
  AND json_is_null(payload)"#
    );
    assert_single_event(instance, &lifecycle).await;
}
