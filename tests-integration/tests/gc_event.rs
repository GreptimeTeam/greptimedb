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

use common_meta::rpc::ddl::{EventContext, TriggerReason};
use common_procedure::{ProcedureId, ProcedureWithId, watcher};
use common_test_util::temp_dir::create_temp_dir;
use meta_srv::gc::{BatchGcProcedure, GcSchedulerOptions};
use mito2::gc::GcConfig;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use tests_integration::test_util::{StorageType, get_test_store_config};

use crate::event_recorder_test_util::find_eventually_string;

const TABLE_NAME: &str = "batch_gc_events";
const MAX_ATTEMPTS: usize = 60;
const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_gc_event() {
    let store_type = StorageType::File;
    if !store_type.test_on() {
        return;
    }

    common_telemetry::init_default_ut_logging();
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_batch_gc_event_data_home");
    let cluster = GreptimeDbClusterBuilder::new("test_batch_gc_event")
        .await
        .with_datanodes(1)
        .with_store_config(store_config)
        .with_shared_home_dir(Arc::new(home_dir))
        .with_metasrv_gc_config(GcSchedulerOptions {
            enable: true,
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

    execute(
        instance,
        &format!(
            "CREATE TABLE {TABLE_NAME} (\
                ts TIMESTAMP TIME INDEX, \
                val DOUBLE, \
                host STRING\
            ) WITH (append_mode = 'true')"
        ),
    )
    .await;
    for day in 1..=4 {
        execute(
            instance,
            &format!(
                "INSERT INTO {TABLE_NAME} VALUES \
                 ('2023-01-{day:02} 10:00:00', {day}.0, 'host{day}')"
            ),
        )
        .await;
        execute(instance, &format!("ADMIN FLUSH_TABLE('{TABLE_NAME}')")).await;
    }
    assert_sst_count(&cluster, 4).await;
    let mut deleted_file_ids = cluster
        .list_sst_files_from_all_datanodes()
        .await
        .into_iter()
        .map(|path| {
            path.rsplit('/')
                .next()
                .unwrap()
                .strip_suffix(".parquet")
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();

    execute(instance, &format!("ADMIN COMPACT_TABLE('{TABLE_NAME}')")).await;
    assert_sst_count(&cluster, 5).await;

    let table = instance
        .catalog_manager()
        .table("greptime", "public", TABLE_NAME, None)
        .await
        .unwrap()
        .unwrap();
    let table_id = table.table_info().table_id();
    let (_, route) = cluster
        .metasrv
        .table_metadata_manager()
        .table_route_manager()
        .get_physical_table_route(table_id)
        .await
        .unwrap();
    let regions = route
        .region_routes
        .iter()
        .map(|route| route.region.id)
        .collect::<Vec<_>>();
    assert_eq!(regions.len(), 1);

    let procedure = BatchGcProcedure::new(
        cluster.metasrv.mailbox().clone(),
        cluster.metasrv.table_metadata_manager().clone(),
        cluster.metasrv.options().grpc.server_addr.clone(),
        regions,
        false,
        Duration::from_secs(10),
        Default::default(),
        EventContext::new(TriggerReason::Manual),
    );
    let procedure_id = ProcedureId::parse_str("00000000-0000-0000-0000-00000000bac0").unwrap();
    let mut watcher = cluster
        .metasrv
        .procedure_manager()
        .submit(ProcedureWithId {
            id: procedure_id,
            procedure: Box::new(procedure),
        })
        .await
        .unwrap();
    watcher::wait(&mut watcher).await.unwrap();
    assert_sst_count(&cluster, 1).await;

    let submitted = format!(
        r#"SELECT json_to_string(event_context) AS event_context
FROM greptime_private.events
WHERE type = 'batch_gc'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Running'
  AND json_get_string(procedure_trigger, 'type') = 'Submitted'"#,
    );
    assert_eq!(
        r#"{"reason":"manual"}"#,
        find_eventually_string(instance, &submitted, "event_context").await
    );

    let succeeded = format!(
        r#"SELECT json_to_string(gc_report) AS gc_report
FROM greptime_private.events
WHERE type = 'batch_gc'
  AND procedure_id = '{procedure_id}'
  AND procedure_state = 'Done'
  AND json_path_match(procedure_trigger, '$.type == "Succeeded"')
  AND json_is_null(payload)
  AND event_context IS NULL"#,
    );
    let actual_report: serde_json::Value =
        serde_json::from_str(&find_eventually_string(instance, &succeeded, "gc_report").await)
            .unwrap();
    assert_eq!(actual_report["need_retry"], false);

    let mut actual_file_ids = actual_report["deleted_files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file_id| file_id.as_str().unwrap())
        .collect::<Vec<_>>();
    actual_file_ids.sort_unstable();
    deleted_file_ids.sort_unstable();
    assert_eq!(actual_file_ids, deleted_file_ids);
}

async fn execute(instance: &Arc<frontend::instance::Instance>, sql: &str) {
    instance
        .do_query(sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
}

async fn assert_sst_count(cluster: &GreptimeDbCluster, expected: usize) {
    let mut last_actual = 0;
    for _ in 0..MAX_ATTEMPTS {
        last_actual = cluster.list_sst_files_from_all_datanodes().await.len();
        if last_actual == expected {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    panic!("timed out waiting for {expected} SST files, found {last_actual}");
}
