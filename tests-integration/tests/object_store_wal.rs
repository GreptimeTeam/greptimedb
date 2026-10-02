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

use std::path::Path;

use common_procedure::options::ProcedureConfig;
use common_query::Output;
use common_wal::config::DatanodeWalConfig;
use common_wal::config::object_store::ObjectStoreWalConfig;
use frontend::instance::Instance;
use object_store::ObjectStore;
use object_store::config::ObjectStoreConfig;
use object_store::services::{Fs, S3};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::standalone::{GreptimeDbStandalone, GreptimeDbStandaloneBuilder};
use tests_integration::test_util::StorageType;

/// The root the store derives its node prefix from.
const WAL_ROOT: &str = "cluster-a/wal";
const BATCHES: usize = 5;
const ROWS_PER_BATCH: usize = 4;
const QUERY: &str = "SELECT hostname, usage_user, ts FROM cpu ORDER BY ts";

async fn execute_sql(instance: &Instance, sql: &str) -> Output {
    SqlQueryHandler::do_query(instance, sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap()
}

async fn query_rows(standalone: &GreptimeDbStandalone) -> String {
    execute_sql(standalone.fe_instance(), QUERY)
        .await
        .data
        .pretty_print()
        .await
}

/// Returns the number of WAL objects under [`WAL_ROOT`] of the default store.
async fn count_wal_objects(standalone: &GreptimeDbStandalone) -> usize {
    let store = match &standalone.opts.storage.store {
        ObjectStoreConfig::File(_) => {
            ObjectStore::new(Fs::default().root(&standalone.opts.storage.data_home)).unwrap()
        }
        ObjectStoreConfig::S3(s3) => ObjectStore::new(S3::from(&s3.connection)).unwrap(),
        other => panic!("unexpected store {other:?}"),
    };
    store
        .list_with(&format!("{WAL_ROOT}/"))
        .recursive(true)
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| entry.metadata().is_file())
        .count()
}

/// Stops the procedure manager, the region server and the WAL of the
/// instance.
async fn shutdown(standalone: &mut GreptimeDbStandalone) {
    standalone.procedure_manager.stop().await.unwrap();
    standalone.datanode.shutdown().await.unwrap();
}

/// Shuts the instance down and builds it again on its metadata, data home and
/// object store, like a process restart.
async fn restart(
    builder: &GreptimeDbStandaloneBuilder,
    mut standalone: GreptimeDbStandalone,
) -> GreptimeDbStandalone {
    shutdown(&mut standalone).await;
    let GreptimeDbStandalone {
        opts,
        guard,
        kv_backend,
        ..
    } = standalone;

    let (procedure_manager, event_recorder_handle) =
        standalone::build_procedure_manager(kv_backend.clone(), ProcedureConfig::default());
    builder
        .build_with(
            kv_backend,
            guard,
            opts,
            procedure_manager,
            event_recorder_handle,
            true,
        )
        .await
}

/// Writes batches that land in separate WAL objects, restarts before and
/// after a flush, and checks that the rows survive both restarts.
async fn run_restarts_on_object_store_wal(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let wal_config = ObjectStoreWalConfig {
        prefix: WAL_ROOT.to_string(),
        ..Default::default()
    };
    let builder = GreptimeDbStandaloneBuilder::new("object_store_wal")
        .with_default_store_type(store_type)
        .with_datanode_wal_config(DatanodeWalConfig::ObjectStore(wal_config));
    let standalone = builder.build().await;

    execute_sql(
        standalone.fe_instance(),
        r#"
        CREATE TABLE cpu (
            hostname STRING PRIMARY KEY,
            usage_user DOUBLE,
            ts TIMESTAMP TIME INDEX
        )
        "#,
    )
    .await;
    // Every insert returns once its entries are durable, so the batches land
    // in separate WAL objects after the object that started the epoch.
    for batch in 0..BATCHES {
        let values = (0..ROWS_PER_BATCH)
            .map(|row| {
                let i = batch * ROWS_PER_BATCH + row;
                format!(
                    "('host_{i}', {i}.0, {})",
                    1_686_567_600_000 + i as i64 * 1000
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        execute_sql(
            standalone.fe_instance(),
            &format!("INSERT INTO cpu VALUES {values}"),
        )
        .await;
    }
    let expected = query_rows(&standalone).await;
    assert_eq!(BATCHES * ROWS_PER_BATCH + 4, expected.lines().count());
    let objects = count_wal_objects(&standalone).await;
    assert!(
        objects > BATCHES,
        "expected more than {BATCHES} WAL objects"
    );
    // No Raft Engine log store is created as a fallback.
    assert!(
        !Path::new(&standalone.opts.storage.data_home)
            .join("wal")
            .exists()
    );

    // Nothing was flushed, so the rows come back from the WAL alone; the
    // reopened store starts its epoch with another object.
    let standalone = restart(&builder, standalone).await;
    assert_eq!(expected, query_rows(&standalone).await);
    assert!(count_wal_objects(&standalone).await > objects);

    // The flushed rows come back from the SST files.
    execute_sql(standalone.fe_instance(), "ADMIN FLUSH_TABLE('cpu')").await;
    let mut standalone = restart(&builder, standalone).await;
    assert_eq!(expected, query_rows(&standalone).await);
    shutdown(&mut standalone).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_standalone_object_store_wal_round_trip() {
    run_restarts_on_object_store_wal(StorageType::File).await;
}

/// Runs against the S3 bucket of the `GT_S3_*` environment variables, so it
/// is skipped unless they are set.
#[tokio::test(flavor = "multi_thread")]
async fn test_standalone_object_store_wal_survives_restarts_on_s3() {
    if !StorageType::S3.test_on() {
        return;
    }
    run_restarts_on_object_store_wal(StorageType::S3).await;
}
