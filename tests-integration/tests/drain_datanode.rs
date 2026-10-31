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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::DatanodeId;
use common_procedure::{ProcedureContext, ProcedureState};
use common_query::{Output, OutputData};
use common_test_util::temp_dir::{TempDir, create_temp_dir};
use common_wal::config::kafka::common::{KafkaConnectionConfig, KafkaTopicConfig};
use common_wal::config::kafka::{DatanodeKafkaConfig, MetasrvKafkaConfig};
use common_wal::config::{DatanodeWalConfig, MetasrvWalConfig};
use meta_srv::key::NodeSchedulingRole;
use meta_srv::procedure::drain_datanode::{DrainDatanodeProcedureTask, DrainDatanodeStrategy};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use store_api::storage::RegionId;
use table::metadata::TableId;
use tests_integration::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use tests_integration::test_util::{StorageType, get_test_store_config};
use uuid::Uuid;

const TIMEOUT: Duration = Duration::from_secs(60);
const REGION_COUNT: usize = 64;
const ROWS_PER_REGION: usize = 128;

#[tokio::test(flavor = "multi_thread")]
async fn test_drain_datanode_pinned_target_multiple_batches() {
    common_wal::maybe_skip_kafka_integration_test!();
    drain_regular_tables("drain_pinned", true, 1, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drain_datanode_auto_target_and_scheduling_fence() {
    common_wal::maybe_skip_kafka_integration_test!();
    drain_regular_tables("drain_auto", false, 2, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drain_datanode_local_wal_pinned_target_multiple_batches() {
    drain_regular_tables("drain_local_pinned", true, 1, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drain_datanode_local_wal_auto_target_and_scheduling_fence() {
    drain_regular_tables("drain_local_auto", false, 2, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drain_datanode_local_wal_metric_tables_remain_readable_and_writable() {
    drain_metric_tables(false).await;
}

async fn build_cluster(name: &str, kafka: bool) -> (GreptimeDbCluster, Arc<TempDir>) {
    common_telemetry::init_default_ut_logging();
    let (store_config, _guard) = get_test_store_config(&StorageType::File);
    let home = Arc::new(create_temp_dir(name));
    let mut builder = GreptimeDbClusterBuilder::new(name)
        .await
        .with_datanodes(4)
        .with_store_config(store_config)
        .with_shared_home_dir(home.clone());
    if kafka {
        let endpoints = common_wal::test_util::get_kafka_endpoints();
        builder = builder
            .with_datanode_wal_config(DatanodeWalConfig::Kafka(DatanodeKafkaConfig {
                connection: KafkaConnectionConfig {
                    broker_endpoints: endpoints.clone(),
                    ..Default::default()
                },
                ..Default::default()
            }))
            .with_metasrv_wal_config(MetasrvWalConfig::Kafka(MetasrvKafkaConfig {
                connection: KafkaConnectionConfig {
                    broker_endpoints: endpoints,
                    ..Default::default()
                },
                kafka_topic: KafkaTopicConfig {
                    num_topics: 3,
                    topic_name_prefix: Uuid::new_v4().to_string(),
                    ..Default::default()
                },
                ..Default::default()
            }));
    }
    (builder.build(true).await, home)
}

async fn sql(cluster: &GreptimeDbCluster, statement: &str) -> Output {
    cluster
        .fe_instance()
        .do_query(statement, QueryContext::arc())
        .await
        .remove(0)
        .unwrap()
}

async fn table_id(cluster: &GreptimeDbCluster, name: &str) -> TableId {
    cluster
        .fe_instance()
        .catalog_manager()
        .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, name, None)
        .await
        .unwrap()
        .unwrap()
        .table_info()
        .table_id()
}

async fn create_regular_table(cluster: &GreptimeDbCluster, name: &str) -> TableId {
    let partitions = (0..REGION_COUNT)
        .map(|i| match i {
            0 => "i < 1".to_string(),
            i if i == REGION_COUNT - 1 => format!("i >= {i}"),
            i => format!("i >= {i} AND i < {}", i + 1),
        })
        .collect::<Vec<_>>()
        .join(", ");
    sql(
        cluster,
        &format!(
            "CREATE TABLE {name} (i INT PRIMARY KEY, v BIGINT, ts TIMESTAMP TIME INDEX) \
         PARTITION ON COLUMNS (i) ({partitions})"
        ),
    )
    .await;
    table_id(cluster, name).await
}

async fn leaders(
    cluster: &GreptimeDbCluster,
    tables: &[TableId],
) -> BTreeMap<RegionId, DatanodeId> {
    let mut leaders = BTreeMap::new();
    for &table_id in tables {
        let distribution = cluster
            .metasrv
            .table_metadata_manager()
            .table_route_manager()
            .get_region_distribution(table_id)
            .await
            .unwrap()
            .unwrap();
        for (node_id, regions) in distribution {
            for region_number in regions.leader_regions {
                assert!(
                    leaders
                        .insert(RegionId::new(table_id, region_number), node_id)
                        .is_none()
                );
            }
        }
    }
    leaders
}

async fn drain(
    cluster: &GreptimeDbCluster,
    source: DatanodeId,
    target: Option<DatanodeId>,
    parallelism: usize,
) {
    let id = cluster
        .metasrv
        .drain_datanode_manager()
        .submit_procedure(
            ProcedureContext::default(),
            DrainDatanodeProcedureTask {
                node_id: source,
                target_node_id: target,
                parallelism,
                strategy: DrainDatanodeStrategy::FailFast,
                timeout: Some(TIMEOUT),
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let state = cluster
                .metasrv
                .procedure_manager()
                .procedure_state(id)
                .await
                .unwrap();
            match state {
                Some(ProcedureState::Done { .. }) => break,
                Some(ProcedureState::Failed { .. } | ProcedureState::Poisoned { .. }) => {
                    panic!("drain {id} failed: {state:?}");
                }
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("drain {id} timed out for datanode {source}"));
    assert!(
        !cluster
            .metasrv
            .node_scheduling_manager()
            .is_schedulable(NodeSchedulingRole::Datanode, source)
            .await
            .unwrap()
    );
}

async fn assert_drained(
    cluster: &GreptimeDbCluster,
    tables: &[TableId],
    before: &BTreeMap<RegionId, DatanodeId>,
    source: DatanodeId,
    target: Option<DatanodeId>,
    excluded: Option<DatanodeId>,
) {
    let after = leaders(cluster, tables).await;
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    for (region, old_leader) in before {
        let new_leader = after[region];
        if *old_leader == source {
            assert_ne!(source, new_leader);
            assert_ne!(excluded, Some(new_leader));
            if let Some(target) = target {
                assert_eq!(target, new_leader);
            }
        } else {
            assert_eq!(*old_leader, new_leader, "unrelated region {region} moved");
        }
    }
}

async fn write_regular_rows(cluster: &GreptimeDbCluster, name: &str, timestamp: i64) {
    let values = (0..REGION_COUNT)
        .flat_map(|i| {
            (0..ROWS_PER_REGION).map(move |j| {
                format!(
                    "({i}, {}, {})",
                    i * ROWS_PER_REGION + j,
                    timestamp + j as i64
                )
            })
        })
        .collect::<Vec<_>>();
    for batch in values.chunks(512) {
        let result = sql(
            cluster,
            &format!("INSERT INTO {name} (i, v, ts) VALUES {}", batch.join(",")),
        )
        .await;
        assert!(matches!(result.data, OutputData::AffectedRows(n) if n == batch.len()));
    }
}

fn expected_count(count: usize) -> String {
    let width = count.to_string().len();
    let border = format!("+{}+", "-".repeat(width + 2));
    format!(
        "{border}\n| {:width$} |\n{border}\n| {count} |\n{border}",
        "n"
    )
}

async fn assert_regular_rows(cluster: &GreptimeDbCluster, name: &str, timestamp: i64) {
    // Each row has a deterministic value tied to its partition and timestamp.
    // Together with the count and primary key this verifies the complete batch.
    let end = timestamp + ROWS_PER_REGION as i64;
    for i in 0..REGION_COUNT {
        let rows = sql(cluster, &format!(
            "SELECT count(*) AS n FROM {name} WHERE i = {i} AND ts >= {timestamp} AND ts < {end} \
             AND v = {} + (CAST(ts AS BIGINT) - {timestamp})",
            i * ROWS_PER_REGION,
        )).await.data.pretty_print().await;
        assert_eq!(
            expected_count(ROWS_PER_REGION),
            rows,
            "unexpected data in {name}, partition {i}, batch {timestamp}"
        );
    }
    let rows = sql(
        cluster,
        &format!("SELECT count(*) AS n FROM {name} WHERE ts >= {timestamp} AND ts < {end}"),
    )
    .await
    .data
    .pretty_print()
    .await;
    assert_eq!(expected_count(REGION_COUNT * ROWS_PER_REGION), rows);
}

async fn drain_regular_tables(name: &str, pinned: bool, parallelism: usize, kafka: bool) {
    let (cluster, _home) = build_cluster(name, kafka).await;
    let names = ["drain_a", "drain_b"];
    let mut tables = Vec::new();
    for name in names {
        tables.push(create_regular_table(&cluster, name).await);
        write_regular_rows(&cluster, name, 0).await;
        assert_regular_rows(&cluster, name, 0).await;
    }
    let before = leaders(&cluster, &tables).await;
    assert_eq!(REGION_COUNT * names.len(), before.len());
    let mut counts = BTreeMap::new();
    for &node_id in before.values() {
        *counts.entry(node_id).or_insert(0usize) += 1;
    }
    let (&source, &count) = counts.iter().max_by_key(|(_, count)| *count).unwrap();
    assert!(count > parallelism, "fixture must require multiple batches");
    let mut others: Vec<_> = cluster
        .datanode_instances
        .keys()
        .copied()
        .filter(|id| *id != source)
        .collect();
    others.sort_unstable();
    let target = pinned.then_some(others[0]);
    let excluded = others[1];
    cluster
        .metasrv
        .node_scheduling_manager()
        .mark_unschedulable(NodeSchedulingRole::Datanode, excluded)
        .await
        .unwrap();

    drain(&cluster, source, target, parallelism).await;
    assert_drained(&cluster, &tables, &before, source, target, Some(excluded)).await;
    for name in names {
        assert_regular_rows(&cluster, name, 0).await;
        write_regular_rows(&cluster, name, 1000).await;
        assert_regular_rows(&cluster, name, 0).await;
        assert_regular_rows(&cluster, name, 1000).await;
    }

    // Uses the cluster's real selector, including its scheduling-state filter.
    let new_table = create_regular_table(&cluster, "after_drain").await;
    let placement = leaders(&cluster, &[new_table]).await;
    assert_eq!(REGION_COUNT, placement.len());
    assert!(
        placement
            .values()
            .all(|&node| node != source && node != excluded)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drain_datanode_metric_tables_remain_readable_and_writable() {
    common_wal::maybe_skip_kafka_integration_test!();
    drain_metric_tables(true).await;
}

async fn drain_metric_tables(kafka: bool) {
    let (cluster, _home) = build_cluster("drain_metric", kafka).await;
    sql(&cluster, r#"CREATE TABLE phy (ts TIMESTAMP TIME INDEX, v DOUBLE) ENGINE=metric WITH ("physical_metric_table"="")"#).await;
    for (name, value) in [("logical_a", 10), ("logical_b", 30)] {
        sql(&cluster, &format!(r#"CREATE TABLE {name} (host STRING PRIMARY KEY, v DOUBLE, ts TIMESTAMP TIME INDEX) ENGINE=metric WITH ("on_physical_table"="phy")"#)).await;
        let output = sql(
            &cluster,
            &format!(
                "INSERT INTO {name} (host,v,ts) VALUES ('a',{value},0),('b',{},0)",
                value + 10
            ),
        )
        .await;
        assert!(matches!(output.data, OutputData::AffectedRows(2)));
    }
    let tables = [table_id(&cluster, "phy").await];
    let before = leaders(&cluster, &tables).await;
    let source = *before.values().next().unwrap();
    let target = *cluster
        .datanode_instances
        .keys()
        .filter(|&&id| id != source)
        .min()
        .unwrap();
    drain(&cluster, source, Some(target), 1).await;
    assert_drained(&cluster, &tables, &before, source, Some(target), None).await;
    for (name, value) in [("logical_a", 10), ("logical_b", 30)] {
        let expected = format!(
            "+------+------+\n| host | v    |\n+------+------+\n| a    | {value}.0 |\n| b    | {}.0 |\n+------+------+",
            value + 10
        );
        for timestamp in [0, 1000] {
            if timestamp != 0 {
                let output = sql(&cluster, &format!("INSERT INTO {name} (host,v,ts) VALUES ('a',{value},{timestamp}),('b',{}, {timestamp})", value + 10)).await;
                assert!(matches!(output.data, OutputData::AffectedRows(2)));
            }
            let rows = sql(
                &cluster,
                &format!("SELECT host,v FROM {name} WHERE ts={timestamp} ORDER BY host"),
            )
            .await
            .data
            .pretty_print()
            .await;
            assert_eq!(expected, rows);
        }
    }
}
