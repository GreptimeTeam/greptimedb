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

use common_query::Output;
use common_telemetry::info;
use common_test_util::recordbatch::check_output_stream;
use common_test_util::temp_dir::create_temp_dir;
use common_wal::config::DatanodeWalConfig;
use frontend::error::Result as FrontendResult;
use frontend::instance::Instance;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::test_util::{StorageType, get_test_store_config};

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
                            $crate::repartition::test_repartition_mito(store_type).await
                        }
                    }
                }
            }
        )*
    };
}

pub async fn test_repartition_mito(store_type: StorageType) {
    let cluster_name = "test_repartition_mito";
    let (store_config, _guard) = get_test_store_config(&store_type);
    let datanodes = 3u64;
    let home_dir = create_temp_dir("test_repartition_mito_data_home");
    let builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
        .with_shared_home_dir(Arc::new(home_dir))
        .with_datanode_wal_config(DatanodeWalConfig::Noop)
        .build(true)
        .await;

    let query_ctx = QueryContext::arc();
    let instance = cluster.fe_instance();

    // 1. Setup: Create a table with partitions
    let sql = r#"
        CREATE TABLE `repartition_mito_table`(
          `id` INT,
          `city` STRING,
          `ts` TIMESTAMP TIME INDEX,
          PRIMARY KEY(`id`, `city`)
        ) PARTITION ON COLUMNS (`id`) (
          `id` < 10,
          `id` >= 10 AND `id` < 20,
          `id` >= 20
        );
    "#;
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

    let result = run_sql(
        instance,
        "SHOW CREATE TABLE `repartition_mito_table`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected_create_table_after_split = r#"+------------------------+-------------------------------------------------------+
| Table                  | Create Table                                          |
+------------------------+-------------------------------------------------------+
| repartition_mito_table | CREATE TABLE IF NOT EXISTS "repartition_mito_table" ( |
|                        |   "id" INT NULL,                                      |
|                        |   "city" STRING NULL,                                 |
|                        |   "ts" TIMESTAMP(3) NOT NULL,                         |
|                        |   TIME INDEX ("ts"),                                  |
|                        |   PRIMARY KEY ("id", "city")                          |
|                        | )                                                     |
|                        | PARTITION ON COLUMNS ("id") (                         |
|                        |   id < 5,                                             |
|                        |   id >= 10 AND id < 20,                               |
|                        |   id >= 20,                                           |
|                        |   id >= 5 AND id < 10                                 |
|                        | )                                                     |
|                        | ENGINE=mito                                           |
|                        |                                                       |
+------------------------+-------------------------------------------------------+"#;
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

    // FIXME(weny): There are some data missing in the result.
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
+----+----------+---------------------+";
    check_output_stream(result.data, expected_all).await;

    let result = run_sql(
        instance,
        "SHOW CREATE TABLE `repartition_mito_table`",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    let expected_create_table_after_merge = r#"+------------------------+-------------------------------------------------------+
| Table                  | Create Table                                          |
+------------------------+-------------------------------------------------------+
| repartition_mito_table | CREATE TABLE IF NOT EXISTS "repartition_mito_table" ( |
|                        |   "id" INT NULL,                                      |
|                        |   "city" STRING NULL,                                 |
|                        |   "ts" TIMESTAMP(3) NOT NULL,                         |
|                        |   TIME INDEX ("ts"),                                  |
|                        |   PRIMARY KEY ("id", "city")                          |
|                        | )                                                     |
|                        | PARTITION ON COLUMNS ("id") (                         |
|                        |   id < 5,                                             |
|                        |   id >= 10 AND id < 20 OR id >= 20,                   |
|                        |   id >= 5 AND id < 10                                 |
|                        | )                                                     |
|                        | ENGINE=mito                                           |
|                        |                                                       |
+------------------------+-------------------------------------------------------+"#;
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

async fn run_sql(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> FrontendResult<Output> {
    info!("Run SQL: {sql}");
    instance.do_query(sql, query_ctx).await.remove(0)
}
