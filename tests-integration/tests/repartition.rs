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

use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::key::table_name::TableNameKey;
use common_procedure::{ProcedureWithId, watcher};
use common_query::Output;
use common_telemetry::info;
use common_test_util::recordbatch::check_output_stream;
use common_test_util::temp_dir::create_temp_dir;
use common_wal::config::DatanodeWalConfig;
use frontend::instance::Instance;
use meta_srv::gc::{self, BatchGcProcedure, GcSchedulerOptions, GcTickerRef};
use meta_srv::metasrv::Metasrv;
use mito2::gc::GcConfig;
use servers::error::Result as ServerResult;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use store_api::codec::PrimaryKeyEncoding;
use store_api::storage::RegionId;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::test_util::{StorageType, get_test_store_config};
use tokio::sync::oneshot;

#[macro_export]
macro_rules! repartition_tests {
    ($($service:ident),*) => {
        $(
            paste::item! {
                mod [<integration_repartition_ $service:lower _test>] {
                    #[tokio::test(flavor = "multi_thread")]
                    async fn [< test_repartition_mito >]() {
                        let store_type = tests_integration::test_util::StorageType::$service;
                        if store_type.test_on() {
                            common_telemetry::init_default_ut_logging();
                            // Cover both storage formats for repartition behavior.
                            // for flat format
                            $crate::repartition::test_repartition_mito(store_type, true).await;
                            // for primary key format
                            $crate::repartition::test_repartition_mito(store_type, false).await;
                        }
                    }

                    #[tokio::test(flavor = "multi_thread")]
                    async fn [< test_repartition_metric >]() {
                        let store_type = tests_integration::test_util::StorageType::$service;
                        if store_type.test_on() {
                            use store_api::codec::PrimaryKeyEncoding;
                            common_telemetry::init_default_ut_logging();
                            // Exercise format + primary key encoding matrix for metric engine.
                            // for flat format with sparse primary key encoding
                            $crate::repartition::test_repartition_metric(store_type, true, PrimaryKeyEncoding::Sparse).await;
                            // for flat format with dense primary key encoding
                            $crate::repartition::test_repartition_metric(store_type, true, PrimaryKeyEncoding::Dense).await;
                            // for primary key format with sparse primary key encoding
                            $crate::repartition::test_repartition_metric(store_type, false, PrimaryKeyEncoding::Sparse).await;
                            // for primary key format with dense primary key encoding
                            $crate::repartition::test_repartition_metric(store_type, false, PrimaryKeyEncoding::Dense).await;
                        }
                    }
                }
            }
        )*
    };
}

async fn trigger_table_gc(metasrv: &Arc<Metasrv>, table_name: &str) {
    info!("triggering table gc for table: {}", table_name);
    let table_metadata_manager = metasrv.table_metadata_manager();
    let table_id = table_metadata_manager
        .table_name_manager()
        .get(TableNameKey::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            table_name,
        ))
        .await
        .unwrap()
        .unwrap()
        .table_id();
    let (_, table_route_value) = table_metadata_manager
        .table_route_manager()
        .get_physical_table_route(table_id)
        .await
        .unwrap();
    let region_ids = table_route_value
        .region_routes
        .iter()
        .map(|r| r.region.id)
        .collect::<Vec<_>>();
    let procedure = BatchGcProcedure::new(
        metasrv.mailbox().clone(),
        metasrv.table_metadata_manager().clone(),
        metasrv.options().grpc.server_addr.clone(),
        region_ids.clone(),
        false,                   // full_file_listing
        Duration::from_secs(10), // timeout
        Default::default(),
    );

    // Submit the procedure to the procedure manager
    let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
    let mut watcher = metasrv
        .procedure_manager()
        .submit(procedure_with_id)
        .await
        .unwrap();
    watcher::wait(&mut watcher).await.unwrap();
}

async fn trigger_full_gc(ticker: &GcTickerRef) {
    info!("triggering full gc");
    let (tx, rx) = oneshot::channel();
    ticker.sender.send(gc::Event::Manually(tx)).await.unwrap();
    let _ = rx.await.unwrap();
}

fn query_partitions_sql(table_name: &str) -> String {
    // We query information_schema.partitions to assert repartition results across engines,
    // rather than relying on SHOW CREATE TABLE formatting differences.
    format!(
        "\
SELECT table_catalog, table_schema, table_name, partition_name, partition_expression, \
partition_description, greptime_partition_id, partition_ordinal_position \
FROM information_schema.partitions \
WHERE table_name = '{}' \
ORDER BY partition_ordinal_position;",
        table_name
    )
}

pub async fn test_repartition_mito(store_type: StorageType, flat_format: bool) {
    info!(
        "test_repartition_mito: store_type: {:?}, flat_format: {:?}",
        store_type, flat_format
    );
    let cluster_name = "test_repartition_mito";
    let (store_config, _guard) = get_test_store_config(&store_type);
    let datanodes = 3u64;
    let mut builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    if matches!(store_type, StorageType::File) {
        let home_dir = create_temp_dir("test_repartition_mito_data_home");
        builder = builder.with_shared_home_dir(Arc::new(home_dir));
    }

    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
        .with_datanode_wal_config(DatanodeWalConfig::Noop)
        .with_metasrv_gc_config(GcSchedulerOptions {
            enable: true,
            gc_cooldown_period: Duration::from_nanos(1),
            ..Default::default()
        })
        .with_datanode_gc_config(GcConfig {
            enable: true,
            lingering_time: Some(Duration::from_secs(0)),
            unknown_file_lingering_time: Duration::from_secs(0),
            ..Default::default()
        })
        .build(true)
        .await;
    let metasrv = &cluster.metasrv;
    let ticker = metasrv.gc_ticker().unwrap();

    let query_ctx = QueryContext::arc();
    let instance = cluster.fe_instance();

    // 1. Setup: Create a table with partitions (format varies by test case)
    let sql = if flat_format {
        r#"
        CREATE TABLE `repartition_mito_table`(
          `id` INT,
          `city` STRING,
          `ts` TIMESTAMP TIME INDEX,
          PRIMARY KEY(`id`, `city`)
        ) PARTITION ON COLUMNS (`id`) (
          `id` < 10,
          `id` >= 10 AND `id` < 20,
          `id` >= 20
        ) ENGINE = mito 
         WITH (
         'sst_format' = 'flat'
         );
    "#
    } else {
        r#"
        CREATE TABLE `repartition_mito_table`(
          `id` INT,
          `city` STRING,
          `ts` TIMESTAMP TIME INDEX,
          PRIMARY KEY(`id`, `city`)
        ) PARTITION ON COLUMNS (`id`) (
          `id` < 10,
          `id` >= 10 AND `id` < 20,
          `id` >= 20
        ) ENGINE = mito;
    "#
    };
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let sql = r#"
        INSERT INTO `repartition_mito_table` VALUES 
          (1, 'New York', '2022-01-01 00:00:00'),
          (5, 'London', '2022-01-01 00:00:00'),
          (10, 'Paris', '2022-01-01 00:00:00'),
          (15, 'Tokyo', '2022-01-01 00:00:00'),
          (20, 'Beijing', '2022-01-01 00:00:00'),
          (25, 'Shanghai', '2022-01-01 00:00:00');
    "#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let result = run_sql(
        instance,
        "SELECT * FROM `repartition_mito_table` ORDER BY `id`",
        query_ctx.clone(),
    )
    .await
    .unwrap();

    let expected = "\
+----+----------+---------------------+
| id | city     | ts                  |
+----+----------+---------------------+
| 1  | New York | 2022-01-01T00:00:00 |
| 5  | London   | 2022-01-01T00:00:00 |
| 10 | Paris    | 2022-01-01T00:00:00 |
| 15 | Tokyo    | 2022-01-01T00:00:00 |
| 20 | Beijing  | 2022-01-01T00:00:00 |
| 25 | Shanghai | 2022-01-01T00:00:00 |
+----+----------+---------------------+";
    check_output_stream(result.data, expected).await;

    // 2. Split Partition
    let sql = r#"
        ALTER TABLE `repartition_mito_table` SPLIT PARTITION (
          `id` < 10
        ) INTO (
          `id` < 5,
          `id` >= 5 AND `id` < 10
        );
    "#;
    let _result = run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let result = run_sql(
        instance,
        "SELECT * FROM `repartition_mito_table` ORDER BY `id`",
        query_ctx.clone(),
    )
    .await
    .unwrap();

    let expected = "\
+----+----------+---------------------+
| id | city     | ts                  |
+----+----------+---------------------+
| 1  | New York | 2022-01-01T00:00:00 |
| 5  | London   | 2022-01-01T00:00:00 |
| 10 | Paris    | 2022-01-01T00:00:00 |
| 15 | Tokyo    | 2022-01-01T00:00:00 |
| 20 | Beijing  | 2022-01-01T00:00:00 |
| 25 | Shanghai | 2022-01-01T00:00:00 |
+----+----------+---------------------+";
    check_output_stream(result.data, expected).await;

    trigger_table_gc(metasrv, "repartition_mito_table").await;
    // Should be ok before compact.
    let result = run_sql(
        instance,
        "SELECT * FROM `repartition_mito_table` ORDER BY `id`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected).await;
    let sst_files_after_gc = cluster.list_sst_files_from_all_datanodes().await;
    let sst_files_after_gc_manifests = cluster.list_sst_files_from_manifests().await;
    assert_eq!(sst_files_after_gc, sst_files_after_gc_manifests);

    // It should be ok, if we try to compact the table after split partition.
    let compact_sql = "ADMIN COMPACT_TABLE('repartition_mito_table', 'swcs', '3600')";
    let _result = run_sql(instance, compact_sql, query_ctx.clone())
        .await
        .unwrap();

    // Should be no change after compact.
    let result = run_sql(
        instance,
        "SELECT * FROM `repartition_mito_table` ORDER BY `id`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected).await;

    // Trigger GC to clean up the compacted files.
    trigger_table_gc(metasrv, "repartition_mito_table").await;
    // Should be no change after GC.
    let result = run_sql(
        instance,
        "SELECT * FROM `repartition_mito_table` ORDER BY `id`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected).await;
    let sst_files_after_gc = cluster.list_sst_files_from_all_datanodes().await;
    let sst_files_after_gc_manifests = cluster.list_sst_files_from_manifests().await;
    assert_eq!(sst_files_after_gc, sst_files_after_gc_manifests);

    let result = run_sql(
        instance,
        &query_partitions_sql("repartition_mito_table"),
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected_create_table_after_split = r#"+---------------+--------------+------------------------+----------------+----------------------+-----------------------+-----------------------+----------------------------+
| table_catalog | table_schema | table_name             | partition_name | partition_expression | partition_description | greptime_partition_id | partition_ordinal_position |
+---------------+--------------+------------------------+----------------+----------------------+-----------------------+-----------------------+----------------------------+
| greptime      | public       | repartition_mito_table | p0             | id                   | id < 5                | 4398046511104         | 1                          |
| greptime      | public       | repartition_mito_table | p1             | id                   | id >= 10 AND id < 20  | 4398046511105         | 2                          |
| greptime      | public       | repartition_mito_table | p2             | id                   | id >= 20              | 4398046511106         | 3                          |
| greptime      | public       | repartition_mito_table | p3             | id                   | id >= 5 AND id < 10   | 4398046511107         | 4                          |
+---------------+--------------+------------------------+----------------+----------------------+-----------------------+-----------------------+----------------------------+"#;
    check_output_stream(result.data, expected_create_table_after_split).await;

    let sql =
        r#"INSERT INTO `repartition_mito_table` VALUES (2, 'Split1', '2022-01-02 00:00:00');"#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let sql =
        r#"INSERT INTO `repartition_mito_table` VALUES (7, 'Split2', '2022-01-02 00:00:00');"#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let result = run_sql(
        instance,
        "SELECT * FROM `repartition_mito_table` WHERE `id` IN (2, 7) ORDER BY `id`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected_split_inserts = "\
+----+--------+---------------------+
| id | city   | ts                  |
+----+--------+---------------------+
| 2  | Split1 | 2022-01-02T00:00:00 |
| 7  | Split2 | 2022-01-02T00:00:00 |
+----+--------+---------------------+";
    check_output_stream(result.data, expected_split_inserts).await;

    // 3. Merge Partition
    let sql = r#"
        ALTER TABLE `repartition_mito_table` MERGE PARTITION (
          `id` >= 10 AND `id` < 20,
          `id` >= 20
        );
    "#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let result = run_sql(
        instance,
        "SELECT * FROM `repartition_mito_table` ORDER BY `id`",
        query_ctx.clone(),
    )
    .await
    .unwrap();

    let expected_all = "\
+----+----------+---------------------+
| id | city     | ts                  |
+----+----------+---------------------+
| 1  | New York | 2022-01-01T00:00:00 |
| 2  | Split1   | 2022-01-02T00:00:00 |
| 5  | London   | 2022-01-01T00:00:00 |
| 7  | Split2   | 2022-01-02T00:00:00 |
| 10 | Paris    | 2022-01-01T00:00:00 |
| 15 | Tokyo    | 2022-01-01T00:00:00 |
| 20 | Beijing  | 2022-01-01T00:00:00 |
| 25 | Shanghai | 2022-01-01T00:00:00 |
+----+----------+---------------------+";
    check_output_stream(result.data, expected_all).await;

    trigger_table_gc(metasrv, "repartition_mito_table").await;
    // Trigger GC to clean up the compacted files.
    trigger_full_gc(&ticker).await;

    let result = run_sql(
        instance,
        "SELECT * FROM `repartition_mito_table` ORDER BY `id`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected_all).await;
    let sst_files_after_gc = cluster.list_sst_files_from_all_datanodes().await;
    let sst_files_after_gc_manifests = cluster.list_sst_files_from_manifests().await;
    assert_eq!(sst_files_after_gc, sst_files_after_gc_manifests);

    // It should be ok, if we try to compact the table after merge partition.
    let compact_sql = "ADMIN COMPACT_TABLE('repartition_mito_table', 'swcs', '3600')";
    let _result = run_sql(instance, compact_sql, query_ctx.clone())
        .await
        .unwrap();

    // Should be no change after compact.
    let result = run_sql(
        instance,
        "SELECT * FROM `repartition_mito_table` ORDER BY `id`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected_all).await;

    trigger_table_gc(metasrv, "repartition_mito_table").await;
    trigger_full_gc(&ticker).await;

    // Should be no change after GC.
    let result = run_sql(
        instance,
        "SELECT * FROM `repartition_mito_table` ORDER BY `id`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected_all).await;
    let sst_files_after_gc = cluster.list_sst_files_from_all_datanodes().await;
    let sst_files_after_gc_manifests = cluster.list_sst_files_from_manifests().await;
    assert_eq!(sst_files_after_gc, sst_files_after_gc_manifests);

    let result = run_sql(
        instance,
        &query_partitions_sql("repartition_mito_table"),
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected_create_table_after_merge = r#"+---------------+--------------+------------------------+----------------+----------------------+-----------------------+-----------------------+----------------------------+
| table_catalog | table_schema | table_name             | partition_name | partition_expression | partition_description | greptime_partition_id | partition_ordinal_position |
+---------------+--------------+------------------------+----------------+----------------------+-----------------------+-----------------------+----------------------------+
| greptime      | public       | repartition_mito_table | p0             | id                   | id < 5                | 4398046511104         | 1                          |
| greptime      | public       | repartition_mito_table | p1             | id                   | id >= 10              | 4398046511105         | 2                          |
| greptime      | public       | repartition_mito_table | p2             | id                   | id >= 5 AND id < 10   | 4398046511107         | 3                          |
+---------------+--------------+------------------------+----------------+----------------------+-----------------------+-----------------------+----------------------------+"#;
    check_output_stream(result.data, expected_create_table_after_merge).await;

    let sql =
        r#"INSERT INTO `repartition_mito_table` VALUES (12, 'Merge1', '2022-01-03 00:00:00');"#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let sql =
        r#"INSERT INTO `repartition_mito_table` VALUES (30, 'Merge2', '2022-01-03 00:00:00');"#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let result = run_sql(
        instance,
        "SELECT * FROM `repartition_mito_table` WHERE `id` IN (12, 30) ORDER BY `id`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected_merge_inserts = "\
+----+--------+---------------------+
| id | city   | ts                  |
+----+--------+---------------------+
| 12 | Merge1 | 2022-01-03T00:00:00 |
| 30 | Merge2 | 2022-01-03T00:00:00 |
+----+--------+---------------------+";
    check_output_stream(result.data, expected_merge_inserts).await;

    run_sql(
        instance,
        "DROP TABLE `repartition_mito_table`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
}

pub async fn test_repartition_metric(
    store_type: StorageType,
    flat_format: bool,
    primary_key_encoding: PrimaryKeyEncoding,
) {
    info!(
        "test_repartition_metric: store_type: {:?}, flat_format: {:?}, primary_key_encoding: {:?}",
        store_type, flat_format, primary_key_encoding
    );
    let cluster_name = "test_repartition_metric";
    let (store_config, _guard) = get_test_store_config(&store_type);
    let datanodes = 3u64;
    let mut builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    if matches!(store_type, StorageType::File) {
        let home_dir = create_temp_dir("test_repartition_metric_data_home");
        builder = builder.with_shared_home_dir(Arc::new(home_dir));
    }
    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
        .with_datanode_wal_config(DatanodeWalConfig::Noop)
        .with_metasrv_gc_config(GcSchedulerOptions {
            enable: true,
            gc_cooldown_period: Duration::from_nanos(1),
            ..Default::default()
        })
        .with_datanode_gc_config(GcConfig {
            enable: true,
            lingering_time: Some(Duration::from_secs(0)),
            unknown_file_lingering_time: Duration::from_secs(0),
            ..Default::default()
        })
        .build(true)
        .await;
    let metasrv = &cluster.metasrv;
    let ticker = metasrv.gc_ticker().unwrap();

    let query_ctx = QueryContext::arc();
    let instance = cluster.fe_instance();

    // Explicitly configure sst_format and primary key encoding to cover the matrix.
    let sst_format = if flat_format { "flat" } else { "primary_key" };
    let primary_key_encoding = match primary_key_encoding {
        PrimaryKeyEncoding::Dense => "dense",
        PrimaryKeyEncoding::Sparse => "sparse",
    };

    let sql = format!(
        r#"
        CREATE TABLE `repart_phy_metric`(
          `ts` TIMESTAMP TIME INDEX,
          `val` DOUBLE,
          `host` STRING PRIMARY KEY
        ) PARTITION ON COLUMNS (`host`) (
          `host` < 'm',
          `host` >= 'm'
        ) ENGINE = metric 
        WITH (
        "physical_metric_table" = "",
        "memtable.type" = "partition_tree",
        'sst_format' = '{sst_format}',
        "memtable.partition_tree.primary_key_encoding" = "{primary_key_encoding}",
        "index.type" = "inverted",
    );
    "#
    );
    run_sql(instance, &sql, query_ctx.clone()).await.unwrap();

    // A second logical table exercises repartition behavior across multiple logical tables
    // sharing the same physical metric table.
    let sql = r#"
        CREATE TABLE `repart_log_metric`(
          `ts` TIMESTAMP TIME INDEX,
          `val` DOUBLE,
          `host` STRING PRIMARY KEY
        ) ENGINE = metric WITH ("on_physical_table" = "repart_phy_metric");
    "#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let sql = r#"
        CREATE TABLE `repart_log_metric_job`(
          `ts` TIMESTAMP TIME INDEX,
          `val` DOUBLE,
          `job` STRING PRIMARY KEY
        ) ENGINE = metric WITH ("on_physical_table" = "repart_phy_metric");
    "#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let sql = r#"
        INSERT INTO `repart_log_metric` (`host`, `ts`, `val`) VALUES
          ('a_host', '2022-01-01 00:00:00', 1),
          ('z_host', '2022-01-01 00:00:00', 2);
    "#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let result = run_sql(
        instance,
        "SELECT * FROM `repart_log_metric` ORDER BY `host`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected = "\
+--------+---------------------+-----+
| host   | ts                  | val |
+--------+---------------------+-----+
| a_host | 2022-01-01T00:00:00 | 1.0 |
| z_host | 2022-01-01T00:00:00 | 2.0 |
+--------+---------------------+-----+";
    check_output_stream(result.data, expected).await;

    // Split physical table partition
    let sql = r#"
        ALTER TABLE `repart_phy_metric` SPLIT PARTITION (
          `host` < 'm'
        ) INTO (
          `host` < 'g',
          `host` >= 'g' AND `host` < 'm'
        );
    "#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let result = run_sql(
        instance,
        &query_partitions_sql("repart_phy_metric"),
        query_ctx.clone(),
    )
    .await
    .unwrap();
    // Partition ids and order are expected to be stable within a single test run.
    let expected_create_table_after_split = r#"+---------------+--------------+-------------------+----------------+----------------------+------------------------+-----------------------+----------------------------+
| table_catalog | table_schema | table_name        | partition_name | partition_expression | partition_description  | greptime_partition_id | partition_ordinal_position |
+---------------+--------------+-------------------+----------------+----------------------+------------------------+-----------------------+----------------------------+
| greptime      | public       | repart_phy_metric | p0             | host                 | host < g               | 4398046511104         | 1                          |
| greptime      | public       | repart_phy_metric | p1             | host                 | host >= m              | 4398046511105         | 2                          |
| greptime      | public       | repart_phy_metric | p2             | host                 | host >= g AND host < m | 4398046511106         | 3                          |
+---------------+--------------+-------------------+----------------+----------------------+------------------------+-----------------------+----------------------------+"#;
    check_output_stream(result.data, expected_create_table_after_split).await;
    let regions = cluster.list_all_regions().await;
    let region0 = regions.get(&RegionId::new(1024, 0)).unwrap();
    let region2 = regions.get(&RegionId::new(1024, 2)).unwrap();
    let primary_keys_in_region_0 = region0
        .metadata()
        .primary_key_columns()
        .cloned()
        .collect::<Vec<_>>();
    let primary_keys_in_region_2 = region2
        .metadata()
        .primary_key_columns()
        .cloned()
        .collect::<Vec<_>>();
    info!("primary_keys_in_region_0: {:?}", primary_keys_in_region_0);
    info!("primary_keys_in_region_2: {:?}", primary_keys_in_region_2);
    assert_eq!(primary_keys_in_region_0, primary_keys_in_region_2);

    let sql = r#"
    ALTER TABLE `repart_log_metric_job` ADD COLUMN `cpu` STRING PRIMARY KEY;
"#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let result = run_sql(
        instance,
        "SELECT * FROM `repart_log_metric` ORDER BY `host`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected = "\
+--------+---------------------+-----+
| host   | ts                  | val |
+--------+---------------------+-----+
| a_host | 2022-01-01T00:00:00 | 1.0 |
| z_host | 2022-01-01T00:00:00 | 2.0 |
+--------+---------------------+-----+";
    check_output_stream(result.data, expected).await;

    trigger_table_gc(metasrv, "repart_phy_metric").await;
    // Should be ok before compact.
    let result = run_sql(
        instance,
        "SELECT * FROM `repart_log_metric` ORDER BY `host`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected).await;
    let sst_files_after_gc = cluster.list_sst_files_from_all_datanodes().await;
    let sst_files_after_gc_manifests = cluster.list_sst_files_from_manifests().await;
    assert_eq!(sst_files_after_gc, sst_files_after_gc_manifests);

    // It should be ok, if we try to compact the table after split partition.
    let compact_sql = "ADMIN COMPACT_TABLE('repart_phy_metric', 'swcs', '3600')";
    let _result = run_sql(instance, compact_sql, query_ctx.clone())
        .await
        .unwrap();

    // Should be no change after compact.
    let result = run_sql(
        instance,
        "SELECT * FROM `repart_log_metric` ORDER BY `host`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected).await;

    // Trigger GC to clean up the compacted files.
    trigger_table_gc(metasrv, "repart_phy_metric").await;
    // Should be no change after GC.
    let result = run_sql(
        instance,
        "SELECT * FROM `repart_log_metric` ORDER BY `host`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected).await;
    let sst_files_after_gc = cluster.list_sst_files_from_all_datanodes().await;
    let sst_files_after_gc_manifests = cluster.list_sst_files_from_manifests().await;
    assert_eq!(sst_files_after_gc, sst_files_after_gc_manifests);

    let sql = r#"INSERT INTO `repart_log_metric` (`host`, `ts`, `val`) VALUES ('b_host', '2022-01-02 00:00:00', 3.0);"#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();
    let sql = r#"INSERT INTO `repart_log_metric` (`host`, `ts`, `val`) VALUES ('h_host', '2022-01-02 00:00:00', 4.0);"#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let result = run_sql(
        instance,
        "SELECT * FROM `repart_log_metric` WHERE `host` IN ('b_host', 'h_host') ORDER BY `host`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected = "\
+--------+---------------------+-----+
| host   | ts                  | val |
+--------+---------------------+-----+
| b_host | 2022-01-02T00:00:00 | 3.0 |
| h_host | 2022-01-02T00:00:00 | 4.0 |
+--------+---------------------+-----+";
    check_output_stream(result.data, expected).await;

    let sql = r#"
        ALTER TABLE `repart_phy_metric` MERGE PARTITION (
          `host` < 'g',
          `host` >= 'g' AND `host` < 'm'
        );
    "#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let result = run_sql(
        instance,
        &query_partitions_sql("repart_phy_metric"),
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected_create_table_after_merge = r#"+---------------+--------------+-------------------+----------------+----------------------+-----------------------+-----------------------+----------------------------+
| table_catalog | table_schema | table_name        | partition_name | partition_expression | partition_description | greptime_partition_id | partition_ordinal_position |
+---------------+--------------+-------------------+----------------+----------------------+-----------------------+-----------------------+----------------------------+
| greptime      | public       | repart_phy_metric | p0             | host                 | host < m              | 4398046511104         | 1                          |
| greptime      | public       | repart_phy_metric | p1             | host                 | host >= m             | 4398046511105         | 2                          |
+---------------+--------------+-------------------+----------------+----------------------+-----------------------+-----------------------+----------------------------+"#;
    check_output_stream(result.data, expected_create_table_after_merge).await;

    let result = run_sql(
        instance,
        "SELECT * FROM `repart_log_metric` ORDER BY `host`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected = "\
+--------+---------------------+-----+
| host   | ts                  | val |
+--------+---------------------+-----+
| a_host | 2022-01-01T00:00:00 | 1.0 |
| b_host | 2022-01-02T00:00:00 | 3.0 |
| h_host | 2022-01-02T00:00:00 | 4.0 |
| z_host | 2022-01-01T00:00:00 | 2.0 |
+--------+---------------------+-----+";
    check_output_stream(result.data, expected).await;

    trigger_table_gc(metasrv, "repart_phy_metric").await;
    trigger_full_gc(&ticker).await;
    // Should be no change after GC.
    let result = run_sql(
        instance,
        "SELECT * FROM `repart_log_metric` ORDER BY `host`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected).await;
    let sst_files_after_gc = cluster.list_sst_files_from_all_datanodes().await;
    let sst_files_after_gc_manifests = cluster.list_sst_files_from_manifests().await;
    assert_eq!(sst_files_after_gc, sst_files_after_gc_manifests);

    // It should be ok, if we try to compact the table after merge partition.
    let compact_sql = "ADMIN COMPACT_TABLE('repart_phy_metric', 'swcs', '3600')";
    let _result = run_sql(instance, compact_sql, query_ctx.clone())
        .await
        .unwrap();

    // Should be no change after compact.
    let result = run_sql(
        instance,
        "SELECT * FROM `repart_log_metric` ORDER BY `host`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected).await;

    // Trigger GC to clean up the compacted files.
    trigger_table_gc(metasrv, "repart_phy_metric").await;
    trigger_full_gc(&ticker).await;

    // Should be no change after GC.
    let result = run_sql(
        instance,
        "SELECT * FROM `repart_log_metric` ORDER BY `host`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    check_output_stream(result.data, expected).await;
    let sst_files_after_gc = cluster.list_sst_files_from_all_datanodes().await;
    let sst_files_after_gc_manifests = cluster.list_sst_files_from_manifests().await;
    assert_eq!(sst_files_after_gc, sst_files_after_gc_manifests);

    let sql = r#"INSERT INTO `repart_log_metric` (`host`, `ts`, `val`) VALUES ('c_host', '2022-01-03 00:00:00', 5.0);"#;
    run_sql(instance, sql, query_ctx.clone()).await.unwrap();

    let result = run_sql(
        instance,
        "SELECT * FROM `repart_log_metric` WHERE `host` = 'c_host'",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected = "\
+--------+---------------------+-----+
| host   | ts                  | val |
+--------+---------------------+-----+
| c_host | 2022-01-03T00:00:00 | 5.0 |
+--------+---------------------+-----+";
    check_output_stream(result.data, expected).await;
    run_sql(
        instance,
        "DROP TABLE `repart_log_metric_job`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    run_sql(
        instance,
        "DROP TABLE `repart_log_metric`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    run_sql(
        instance,
        "DROP TABLE `repart_phy_metric`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
}

async fn run_sql(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> ServerResult<Output> {
    info!("Run SQL: {sql}");
    instance.do_query(sql, query_ctx).await.remove(0)
}
