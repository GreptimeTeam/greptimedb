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
use common_telemetry::{info, warn};
use common_test_util::recordbatch::check_output_stream;
use common_test_util::temp_dir::create_temp_dir;
use common_wal::config::DatanodeWalConfig;
use frontend::instance::Instance;
use meta_srv::gc::GcSchedulerOptions;
use mito2::gc::GcConfig;
use servers::error::Result as ServerResult;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::test_util::{StorageType, get_test_store_config};
use tokio::time::{Duration, sleep};

#[macro_export]
macro_rules! repartition_expr_version_tests {
    ($($service:ident),*) => {
        $(
            paste::item! {
                mod [<integration_repartition_expr_version_ $service:lower _test>] {
                    #[tokio::test(flavor = "multi_thread")]
                    async fn [< test_repartition_expr_version >]() {
                        let store_type = tests_integration::test_util::StorageType::$service;
                        if store_type.test_on() {
                            common_telemetry::init_default_ut_logging();
                            $crate::repartition_expr_version::test_repartition_expr_version(store_type).await
                        }
                    }
                }
            }
        )*
    };
}

pub async fn test_repartition_expr_version(store_type: StorageType) {
    let cluster_name = "test_repartition_expr_version";
    let (store_config, _guard) = get_test_store_config(&store_type);
    let mut builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    if matches!(store_type, StorageType::File) {
        let home_dir = create_temp_dir("test_repartition_expr_version_data_home");
        builder = builder.with_shared_home_dir(Arc::new(home_dir));
    }

    let cluster = builder
        .with_datanodes(3_u32)
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
    let instance = cluster.fe_instance().clone();
    let query_ctx = QueryContext::arc();

    let sql = r#"
        CREATE TABLE repartition_expr_version_table(
          `id` INT,
          `ts` TIMESTAMP TIME INDEX,
          `val` DOUBLE,
          PRIMARY KEY(`id`)
        ) PARTITION ON COLUMNS (`id`) (
          `id` < 10,
          `id` >= 10
        );
    "#;
    run_sql(&instance, sql, query_ctx.clone()).await.unwrap();

    let mut success_count = 0i64;
    let mut failure_count = 0i64;
    let mut next_id = 0i64;
    for _ in 0..10 {
        let id = next_id;
        next_id += 1;
        let sql = format!(
            "INSERT INTO repartition_expr_version_table VALUES ({id}, '2022-01-01 00:00:00', 0.5)"
        );
        run_sql(&instance, &sql, query_ctx.clone()).await.unwrap();
        success_count += 1;
    }

    let moved_instance = instance.clone();
    let moved_query_ctx = query_ctx.clone();
    let ddl_handle = tokio::spawn(async move {
        run_sql(
            &moved_instance,
            r#"
                ALTER TABLE repartition_expr_version_table SPLIT PARTITION (
                  `id` < 10
                ) INTO (
                  `id` < 5,
                  `id` >= 5 AND `id` < 10
                );
                "#,
            moved_query_ctx.clone(),
        )
        .await
    });

    while !ddl_handle.is_finished() {
        let id = next_id;
        next_id += 1;
        let sql = format!(
            "INSERT INTO repartition_expr_version_table VALUES ({id}, '2022-01-01 00:00:00', 1.0)"
        );
        match run_sql(&instance, &sql, query_ctx.clone()).await {
            Ok(_) => success_count += 1,
            Err(err) => {
                failure_count += 1;
                warn!("ddl-phase insert failed: {err}");
            }
        }
        sleep(Duration::from_millis(50)).await;
    }

    ddl_handle.await.unwrap().unwrap();

    let id = next_id;
    let sql = format!(
        "INSERT INTO repartition_expr_version_table VALUES ({id}, '2022-01-01 00:00:00', 2.0)"
    );
    run_sql(&instance, &sql, query_ctx.clone()).await.unwrap();
    success_count += 1;

    let result = run_sql(
        &instance,
        "SELECT count(*) FROM repartition_expr_version_table",
        query_ctx.clone(),
    )
    .await
    .unwrap();
    info!("inserts: success={success_count}, failure={failure_count}");
    let expected = count_expected(success_count);
    check_output_stream(result.data, expected.as_str()).await;

    run_sql(
        &instance,
        "DROP TABLE repartition_expr_version_table",
        query_ctx,
    )
    .await
    .unwrap();
}

fn count_expected(count: i64) -> String {
    let header = "count(*)";
    let value = count.to_string();
    let width = header.len().max(value.len());
    let border = format!("+{}+", "-".repeat(width + 2));
    format!(
        "{border}\n| {header:<width$} |\n{border}\n| {value:<width$} |\n{border}",
        width = width
    )
}

async fn run_sql(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> ServerResult<Output> {
    info!("Run SQL: {sql}");
    instance.do_query(sql, query_ctx).await.remove(0)
}
