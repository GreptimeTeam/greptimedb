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
use std::sync::Arc;
use std::time::Duration;

use datanode::config::RegionEngineConfig;
use frontend::instance::Instance;
use mito2::config::MitoConfig;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::{
    StorageType, create_tmp_dir_and_datanode_opts, execute_sql, try_execute_sql,
};

fn config() -> MitoConfig {
    MitoConfig {
        experimental_enable_series_index: true,
        experimental_series_index_maintenance_interval: Duration::from_secs(3600),
        ..Default::default()
    }
}

async fn exercise_admin(frontend: &Arc<Instance>) {
    execute_sql(
        frontend,
        r#"
        CREATE TABLE series_physical (ts TIMESTAMP TIME INDEX, host STRING PRIMARY KEY, val DOUBLE)
        PARTITION ON COLUMNS (host) (host < 'm', host >= 'm')
        ENGINE = metric WITH (
            physical_metric_table = 'true', 'compaction.twcs.time_window' = '1h',
            'compaction.twcs.active_window.trigger_file_num' = '100',
            'compaction.twcs.inactive_window.trigger_file_num' = '100'
        )
    "#,
    )
    .await;
    execute_sql(
        frontend,
        r#"
        CREATE TABLE series_logical (ts TIMESTAMP TIME INDEX, host STRING PRIMARY KEY, val DOUBLE)
        ENGINE = metric WITH (on_physical_table = 'series_physical')
    "#,
    )
    .await;
    // Reconciliation requires at least four SSTs in each bucket.
    for timestamp in [1000, 2000, 3000, 4000] {
        execute_sql(
            frontend,
            &format!(
                "INSERT INTO series_logical VALUES ({timestamp}, 'a', 1), ({timestamp}, 'z', 2)"
            ),
        )
        .await;
        execute_sql(frontend, "ADMIN FLUSH_TABLE('series_physical')").await;
    }
    for _ in 0..2 {
        let output = execute_sql(frontend, "ADMIN BUILD_SERIES_INDEX('series_physical')").await;
        assert!(output.data.pretty_print().await.contains("| 0"));
    }
    // The function must never silently expand a logical table to its shared physical table.
    let error = try_execute_sql(frontend, "ADMIN BUILD_SERIES_INDEX('series_logical')")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("physical metric table"));
    for sql in [
        "ADMIN BUILD_SERIES_INDEX()",
        "ADMIN BUILD_SERIES_INDEX(1)",
        "ADMIN BUILD_SERIES_INDEX('missing')",
    ] {
        assert!(try_execute_sql(frontend, sql).await.is_err(), "{sql}");
    }
    execute_sql(frontend, "CREATE TABLE ordinary (ts TIMESTAMP TIME INDEX)").await;
    assert!(
        try_execute_sql(frontend, "ADMIN BUILD_SERIES_INDEX('ordinary')")
            .await
            .is_err()
    );
    execute_sql(frontend, "ADMIN BUILD_INDEX('ordinary')").await;
    let output = execute_sql(frontend, "SELECT count(*) AS n FROM series_logical").await;
    assert!(output.data.pretty_print().await.contains("| 8"));
}

fn catalog_count(home: &str) -> usize {
    let root = Path::new(home).join("series_index");
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.join("series-index.json").is_file())
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_build_series_index_standalone() {
    let standalone = GreptimeDbStandaloneBuilder::new("build-series-index")
        .with_mito_config(config())
        .build()
        .await;
    exercise_admin(standalone.fe_instance()).await;
    assert_eq!(2, catalog_count(&standalone.opts.storage.data_home));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_build_series_index_distributed() {
    let builder = GreptimeDbClusterBuilder::new("build-series-index").await;
    let mut options = Vec::new();
    let mut guards = Vec::new();
    for id in 1..=2 {
        let (mut opts, guard) = create_tmp_dir_and_datanode_opts(
            StorageType::File,
            vec![],
            &format!("build-series-index-{id}"),
            Default::default(),
            Default::default(),
        );
        opts.node_id = Some(id);
        for engine in &mut opts.region_engine {
            if let RegionEngineConfig::Mito(mito) = engine {
                *mito = config();
            }
        }
        options.push(opts);
        guards.push(guard);
    }
    let cluster = builder.build_with(options, false, guards).await;
    exercise_admin(cluster.fe_instance()).await;
    assert_eq!(
        2,
        cluster
            .datanode_options
            .iter()
            .map(|opts| catalog_count(&opts.storage.data_home))
            .sum::<usize>()
    );
}
