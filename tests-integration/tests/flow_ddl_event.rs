// Copyright 2026 Greptime Team
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
use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches};
use common_test_util::temp_dir::create_temp_dir;
use common_time::util::DefaultSystemTimer;
use common_wal::config::DatanodeWalConfig;
use datatypes::arrow::array::{Array, AsArray};
use datatypes::arrow::compute::concat_batches;
use frontend::instance::Instance;
use meta_srv::discovery;
use servers::error::Result as ServerResult;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use tests_integration::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use tests_integration::test_util::{StorageType, get_test_store_config};
use tokio::time::{Duration, Instant, sleep};
use uuid::Uuid;

const CREATE_FLOW_EVENT_TYPE: &str = "ddl_create_flow";
const DROP_FLOW_EVENT_TYPE: &str = "ddl_drop_flow";

#[tokio::test(flavor = "multi_thread")]
async fn test_flow_ddl_events_file() {
    if !StorageType::File.test_on() {
        return;
    }
    common_telemetry::init_default_ut_logging();

    // Arrange: isolate the cluster and all DDL names from concurrently running tests.
    let (store_config, _guard) = get_test_store_config(&StorageType::File);
    let home_dir = create_temp_dir("test_flow_ddl_events_data_home");
    let cluster = GreptimeDbClusterBuilder::new("test_flow_ddl_events")
        .await
        .with_shared_home_dir(Arc::new(home_dir))
        .with_datanodes(1)
        .with_store_config(store_config)
        .with_datanode_wal_config(DatanodeWalConfig::Noop)
        .build(true)
        .await;
    wait_for_frontend(&cluster).await;
    let instance = cluster.fe_instance();
    let suffix = Uuid::new_v4().simple();
    let missing_source = format!("flow_ddl_event_missing_source_{suffix}");
    let sink = format!("flow_ddl_event_sink_{suffix}");
    let flow = format!("flow_ddl_event_{suffix}");

    run_sql(
        instance,
        &format!("CREATE TABLE {sink} (val STRING, ts TIMESTAMP TIME INDEX)"),
        QueryContext::arc(),
    )
    .await
    .unwrap();

    // Act: create a pending flow. The missing source avoids requiring a flownode in this cluster.
    run_sql(
        instance,
        &format!(
            "CREATE FLOW IF NOT EXISTS {flow} SINK TO {sink} EVAL INTERVAL '10s' \
             WITH (defer_on_missing_source = true) AS SELECT val, ts FROM {missing_source}"
        ),
        QueryContext::arc(),
    )
    .await
    .unwrap();
    let create_procedure_id = wait_for_event(instance, CREATE_FLOW_EVENT_TYPE).await;

    // Assert: Submitted retains the bounded intent and locators; Succeeded retains only flow_id.
    assert_create_events(instance, &flow, &create_procedure_id).await;

    // Act: drop with IF EXISTS so the submitted row records the requested intent.
    run_sql(
        instance,
        &format!("DROP FLOW IF EXISTS {flow}"),
        QueryContext::arc(),
    )
    .await
    .unwrap();
    let drop_procedure_id = wait_for_event(instance, DROP_FLOW_EVENT_TYPE).await;

    // Assert: Submitted has its typed drop intent; Succeeded is fully lightweight.
    assert_drop_events(instance, &flow, &drop_procedure_id).await;
}

async fn wait_for_frontend(cluster: &GreptimeDbCluster) {
    let deadline = Instant::now() + Duration::from_secs(15);

    while Instant::now() < deadline {
        let frontends = discovery::utils::alive_frontend_infos(
            &DefaultSystemTimer,
            cluster.metasrv.meta_peer_client().as_ref(),
            Duration::from_secs(u64::MAX),
        )
        .await
        .unwrap();
        if !frontends.is_empty() {
            return;
        }

        sleep(Duration::from_millis(100)).await;
    }

    panic!("timed out waiting for an active frontend");
}

async fn wait_for_event(instance: &Arc<Instance>, event_type: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_result = format!("waiting for a submitted {event_type} event");

    while Instant::now() < deadline {
        match query_procedure_id(
            instance,
            &format!(
                "SELECT procedure_id FROM greptime_private.events \
                 WHERE type = '{event_type}' AND procedure_trigger = 'Submitted' \
                 ORDER BY timestamp DESC LIMIT 1"
            ),
        )
        .await
        {
            Ok(Some(procedure_id)) => {
                let terminal_sql = format!(
                    "SELECT procedure_id FROM greptime_private.events \
                     WHERE type = '{event_type}' AND procedure_id = '{procedure_id}' \
                     AND procedure_trigger = 'Succeeded' LIMIT 1"
                );
                match query_procedure_id(instance, &terminal_sql).await {
                    Ok(Some(_)) => return procedure_id,
                    Ok(None) => last_result = format!("no succeeded {event_type} event found"),
                    Err(error) => last_result = format!("{error:?}"),
                }
            }
            Ok(None) => last_result = format!("no submitted {event_type} event found"),
            Err(error) => last_result = format!("{error:?}"),
        }

        sleep(Duration::from_millis(100)).await;
    }

    panic!("timed out waiting for {event_type} event contract; last result: {last_result}");
}

async fn query_events(
    instance: &Arc<Instance>,
    event_type: &str,
    procedure_id: &str,
) -> RecordBatch {
    let OutputData::Stream(stream) = run_sql(
        instance,
        &format!(
            "SELECT procedure_trigger, catalog_name, schema_name, flow_name, flow_id, payload FROM greptime_private.events WHERE type = '{event_type}' AND procedure_id = '{procedure_id}' ORDER BY timestamp ASC"
        ),
        QueryContext::arc(),
    )
    .await
    .unwrap()
    .data
    else {
        unreachable!();
    };
    let batches = RecordBatches::try_collect(stream).await.unwrap().take();
    let schema = batches.first().unwrap().schema.clone();
    let batch_refs = batches.iter().map(RecordBatch::df_record_batch);
    let df_record_batch = concat_batches(schema.arrow_schema(), batch_refs).unwrap();
    RecordBatch::from_df_record_batch(schema, df_record_batch)
}

async fn assert_create_events(instance: &Arc<Instance>, flow: &str, procedure_id: &str) {
    let events = query_events(instance, CREATE_FLOW_EVENT_TYPE, procedure_id).await;
    assert_eq!(events.num_rows(), 2);

    let trigger = events.column(0).as_string::<i32>();
    let catalog = events.column(1).as_string::<i32>();
    let schema = events.column(2).as_string::<i32>();
    let flow_name = events.column(3).as_string::<i32>();
    let flow_id = events.column(4);
    let payload = events.column(5).as_binary::<i32>();

    assert_eq!(trigger.value(0), "Submitted");
    assert_eq!(catalog.value(0), "greptime");
    assert_eq!(schema.value(0), "public");
    assert_eq!(flow_name.value(0), flow);
    assert!(flow_id.is_null(0));
    let submitted_payload: serde_json::Value = jsonb::from_slice(payload.value(0)).unwrap().into();
    assert_eq!(
        submitted_payload,
        serde_json::json!({
            "version": 1,
            "or_replace": false,
            "create_if_not_exists": true,
            "expire_after": null,
            "eval_interval_secs": 10,
        })
    );

    assert_eq!(trigger.value(1), "Succeeded");
    assert!(catalog.is_null(1));
    assert!(schema.is_null(1));
    assert!(flow_name.is_null(1));
    assert!(!flow_id.is_null(1));
    let succeeded_payload: serde_json::Value = jsonb::from_slice(payload.value(1)).unwrap().into();
    assert_eq!(succeeded_payload, serde_json::Value::Null);
}

async fn assert_drop_events(instance: &Arc<Instance>, flow: &str, procedure_id: &str) {
    let events = query_events(instance, DROP_FLOW_EVENT_TYPE, procedure_id).await;
    assert_eq!(events.num_rows(), 2);

    let trigger = events.column(0).as_string::<i32>();
    let catalog = events.column(1).as_string::<i32>();
    let schema = events.column(2).as_string::<i32>();
    let flow_name = events.column(3).as_string::<i32>();
    let flow_id = events.column(4);
    let payload = events.column(5).as_binary::<i32>();

    assert_eq!(trigger.value(0), "Submitted");
    assert_eq!(catalog.value(0), "greptime");
    assert!(schema.is_null(0));
    assert_eq!(flow_name.value(0), flow);
    assert!(!flow_id.is_null(0));
    let submitted_payload: serde_json::Value = jsonb::from_slice(payload.value(0)).unwrap().into();
    assert_eq!(
        submitted_payload,
        serde_json::json!({"version": 1, "drop_if_exists": true})
    );

    assert_eq!(trigger.value(1), "Succeeded");
    assert!(catalog.is_null(1));
    assert!(schema.is_null(1));
    assert!(flow_name.is_null(1));
    assert!(flow_id.is_null(1));
    let succeeded_payload: serde_json::Value = jsonb::from_slice(payload.value(1)).unwrap().into();
    assert_eq!(succeeded_payload, serde_json::Value::Null);
}

async fn run_sql(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> ServerResult<Output> {
    instance.do_query(sql, query_ctx).await.remove(0)
}

async fn query_procedure_id(instance: &Arc<Instance>, sql: &str) -> ServerResult<Option<String>> {
    let output = instance
        .do_query(sql, QueryContext::arc())
        .await
        .remove(0)?;
    let OutputData::Stream(stream) = output.data else {
        unreachable!("event query must return a stream");
    };
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let Some(batch) = batches.take().into_iter().next() else {
        return Ok(None);
    };
    let Some(column) = batch.column_by_name("procedure_id") else {
        return Ok(None);
    };
    Ok(column
        .as_string::<i32>()
        .iter()
        .next()
        .flatten()
        .map(ToString::to_string))
}
