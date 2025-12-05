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

use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, OutputData};
use common_catalog::consts::DEFAULT_PRIVATE_SCHEMA_NAME;
use common_event_recorder::{
    DEFAULT_EVENTS_TABLE_NAME, DEFAULT_FLUSH_INTERVAL_SECONDS, EVENTS_TABLE_TIMESTAMP_COLUMN_NAME,
    EVENTS_TABLE_TYPE_COLUMN_NAME,
};
use common_meta::key::{RegionDistribution, RegionRoleSet, TableMetadataManagerRef};
use common_meta::peer::Peer;
use common_procedure::event::{
    EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME, EVENTS_TABLE_PROCEDURE_STATE_COLUMN_NAME,
};
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::info;
use common_test_util::recordbatch::check_output_stream;
use common_test_util::temp_dir::create_temp_dir;
use common_wal::config::kafka::common::{KafkaConnectionConfig, KafkaTopicConfig};
use common_wal::config::kafka::{DatanodeKafkaConfig, MetasrvKafkaConfig};
use common_wal::config::{DatanodeWalConfig, MetasrvWalConfig};
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::UInt64Type;
use frontend::error::Result as FrontendResult;
use frontend::instance::Instance;
use futures::future::BoxFuture;
use meta_srv::error;
use meta_srv::error::Result as MetaResult;
use meta_srv::events::region_migration_event::{
    EVENTS_TABLE_DST_NODE_ID_COLUMN_NAME, EVENTS_TABLE_REGION_ID_COLUMN_NAME,
    EVENTS_TABLE_REGION_MIGRATION_TRIGGER_REASON_COLUMN_NAME, EVENTS_TABLE_SRC_NODE_ID_COLUMN_NAME,
    REGION_MIGRATION_EVENT_TYPE,
};
use meta_srv::metasrv::SelectorContext;
use meta_srv::procedure::region_migration::{
    RegionMigrationProcedureTask, RegionMigrationTriggerReason,
};
use meta_srv::selector::{Selector, SelectorOptions};
use sea_query::{Expr, Iden, Order, PostgresQueryBuilder, Query};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use store_api::storage::RegionId;
use table::metadata::TableId;
use tests_integration::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use tests_integration::test_util::{PEER_PLACEHOLDER_ADDR, StorageType, get_test_store_config};
use uuid::Uuid;

const TEST_TABLE_NAME: &str = "migration_target";

#[macro_export]
macro_rules! region_migration_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<integration_region_migration_ $service:lower _test>] {
                $(
                    #[tokio::test(flavor = "multi_thread")]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() {
                        let store_type = tests_integration::test_util::StorageType::$service;
                        if store_type.test_on() {
                            common_telemetry::init_default_ut_logging();
                            common_wal::maybe_skip_kafka_integration_test!();
                            let endpoints = common_wal::test_util::get_kafka_endpoints();
                            $crate::region_migration::$test(store_type, endpoints).await
                        }

                    }
                )*
            }
        }
    };
}

#[macro_export]
macro_rules! region_migration_tests {
    ($($service:ident),*) => {
        $(
            region_migration_test!(
                $service,

                test_region_migration,
                test_region_migration_by_sql,
                test_region_migration_multiple_regions,
                test_region_migration_all_regions,
                test_region_migration_incorrect_from_peer,
                test_region_migration_incorrect_region_id,
                test_metric_table_region_migration_by_sql,
            );
        )*
    };
}

/// A naive region migration test.
pub async fn test_region_migration(store_type: StorageType, endpoints: Vec<String>) {
    let cluster_name = "test_region_migration";
    let peer_factory = |id| Peer {
        id,
        addr: PEER_PLACEHOLDER_ADDR.to_string(),
    };

    // Prepares test cluster.
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_migration_data_home");
    let datanodes = 5u64;
    let builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    let const_selector = Arc::new(ConstNodeSelector::new(vec![
        peer_factory(1),
        peer_factory(2),
        peer_factory(3),
    ]));
    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
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
        }))
        .with_shared_home_dir(Arc::new(home_dir))
        .with_meta_selector(const_selector.clone())
        .build(true)
        .await;
    let mut logical_timer = 1685508715000;
    let table_metadata_manager = cluster.metasrv.table_metadata_manager().clone();

    // Prepares test table.
    let table_id = prepare_testing_table(&cluster).await;

    // Inserts data
    let results = insert_values(cluster.fe_instance(), logical_timer).await;
    logical_timer += 1000;
    for result in results {
        assert!(matches!(result.unwrap().data, OutputData::AffectedRows(1)));
    }

    // The region distribution
    let mut distribution = find_region_distribution(&table_metadata_manager, table_id).await;

    // Selecting target of region migration.
    let region_migration_manager = cluster.metasrv.region_migration_manager();
    let (from_peer_id, from_regions) = distribution.pop_first().unwrap();
    info!(
        "Selecting from peer: {from_peer_id}, and regions: {:?}",
        from_regions
    );
    let (to_peer_id, mut to_regions) = distribution.pop_first().unwrap();
    info!(
        "Selecting to peer: {to_peer_id}, and regions: {:?}",
        to_regions
    );

    let region_id = RegionId::new(table_id, from_regions.leader_regions[0]);
    // Trigger region migration.
    let procedure = region_migration_manager
        .submit_procedure(RegionMigrationProcedureTask::new(
            region_id,
            peer_factory(from_peer_id),
            peer_factory(to_peer_id),
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Manual,
        ))
        .await
        .unwrap();
    info!("Started region procedure: {}!", procedure.unwrap());

    // Prepares expected region distribution.
    to_regions
        .leader_regions
        .extend(from_regions.leader_regions);
    to_regions
        .follower_regions
        .extend(from_regions.follower_regions);
    // Keeps asc order.
    to_regions.sort();
    distribution.insert(to_peer_id, to_regions);

    // Waits condition
    wait_condition(
        Duration::from_secs(10),
        Box::pin(async move {
            loop {
                let region_migration =
                    find_region_distribution(&table_metadata_manager, table_id).await;
                if region_migration == distribution {
                    info!("Found new distribution: {region_migration:?}");
                    break;
                } else {
                    info!("Found the unexpected distribution: {region_migration:?}, expected: {distribution:?}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }),
    )
    .await;

    // Inserts more table.
    let results = insert_values(cluster.fe_instance(), logical_timer).await;
    for result in results {
        assert!(matches!(result.unwrap().data, OutputData::AffectedRows(1)));
    }

    // Asserts the writes.
    assert_values(cluster.fe_instance()).await;

    // Triggers again.
    let err = region_migration_manager
        .submit_procedure(RegionMigrationProcedureTask::new(
            region_id,
            peer_factory(from_peer_id),
            peer_factory(to_peer_id),
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Manual,
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, error::Error::RegionMigrated { .. }));

    check_region_migration_events_system_table(
        cluster.fe_instance(),
        &procedure.unwrap().to_string(),
        region_id.as_u64(),
        from_peer_id,
        to_peer_id,
    )
    .await;
}

/// A naive metric table region migration test by SQL function
pub async fn test_metric_table_region_migration_by_sql(
    store_type: StorageType,
    endpoints: Vec<String>,
) {
    let cluster_name = "test_region_migration";
    let peer_factory = |id| Peer {
        id,
        addr: PEER_PLACEHOLDER_ADDR.to_string(),
    };

    // Prepares test cluster.
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_migration_data_home");
    let datanodes = 5u64;
    let builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    let const_selector = Arc::new(ConstNodeSelector::new(vec![
        peer_factory(1),
        peer_factory(2),
        peer_factory(3),
    ]));
    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
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
        }))
        .with_shared_home_dir(Arc::new(home_dir))
        .with_meta_selector(const_selector.clone())
        .build(true)
        .await;
    // Prepares test metric tables.
    let table_id = prepare_testing_metric_table(&cluster).await;
    let query_ctx = QueryContext::arc();

    // Inserts values
    run_sql(
        cluster.fe_instance(),
        r#"INSERT INTO t1 VALUES ('host1',0, 0), ('host2', 1, 1);"#,
        query_ctx.clone(),
    )
    .await
    .unwrap();

    run_sql(
        cluster.fe_instance(),
        r#"INSERT INTO t2 VALUES ('job1', 0, 0), ('job2', 1, 1);"#,
        query_ctx.clone(),
    )
    .await
    .unwrap();

    // The region distribution
    let mut distribution = find_region_distribution_by_sql(&cluster, "phy").await;
    // Selecting target of region migration.
    let (from_peer_id, from_regions) = distribution.pop_first().unwrap();
    info!(
        "Selecting from peer: {from_peer_id}, and regions: {:?}",
        from_regions.leader_regions[0]
    );
    let to_peer_id = (from_peer_id + 1) % 3;
    let region_id = RegionId::new(table_id, from_regions.leader_regions[0]);
    // Trigger region migration.
    let procedure_id =
        trigger_migration_by_sql(&cluster, region_id.as_u64(), from_peer_id, to_peer_id).await;

    info!("Started region procedure: {}!", procedure_id);

    // Waits condition by checking procedure state
    let frontend = cluster.fe_instance().clone();
    let procedure_id_for_closure = procedure_id.clone();
    wait_condition(
        Duration::from_secs(10),
        Box::pin(async move {
            loop {
                let state = query_procedure_by_sql(&frontend, &procedure_id_for_closure).await;
                if state == "{\"status\":\"Done\"}" {
                    info!("Migration done: {state}");
                    break;
                } else {
                    info!("Migration not finished: {state}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }),
    )
    .await;

    let result = cluster
        .frontend
        .instance
        .do_query("select * from t1 order by host desc", query_ctx.clone())
        .await
        .remove(0);

    let expected = "\
+-------+-------------------------+-----+
| host  | ts                      | val |
+-------+-------------------------+-----+
| host2 | 1970-01-01T00:00:00.001 | 1.0 |
| host1 | 1970-01-01T00:00:00     | 0.0 |
+-------+-------------------------+-----+";
    check_output_stream(result.unwrap().data, expected).await;

    let result = cluster
        .frontend
        .instance
        .do_query("select * from t2 order by job desc", query_ctx)
        .await
        .remove(0);

    let expected = "\
+------+-------------------------+-----+
| job  | ts                      | val |
+------+-------------------------+-----+
| job2 | 1970-01-01T00:00:00.001 | 1.0 |
| job1 | 1970-01-01T00:00:00     | 0.0 |
+------+-------------------------+-----+";
    check_output_stream(result.unwrap().data, expected).await;

    check_region_migration_events_system_table(
        cluster.fe_instance(),
        &procedure_id,
        region_id.as_u64(),
        from_peer_id,
        to_peer_id,
    )
    .await;
}

/// A naive region migration test by SQL function
pub async fn test_region_migration_by_sql(store_type: StorageType, endpoints: Vec<String>) {
    let cluster_name = "test_region_migration";
    let peer_factory = |id| Peer {
        id,
        addr: PEER_PLACEHOLDER_ADDR.to_string(),
    };

    // Prepares test cluster.
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_migration_data_home");
    let datanodes = 5u64;
    let builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    let const_selector = Arc::new(ConstNodeSelector::new(vec![
        peer_factory(1),
        peer_factory(2),
        peer_factory(3),
    ]));
    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
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
        }))
        .with_shared_home_dir(Arc::new(home_dir))
        .with_meta_selector(const_selector.clone())
        .build(true)
        .await;
    let mut logical_timer = 1685508715000;

    // Prepares test table.
    let table_id = prepare_testing_table(&cluster).await;

    // Inserts data
    let results = insert_values(cluster.fe_instance(), logical_timer).await;
    logical_timer += 1000;
    for result in results {
        assert!(matches!(result.unwrap().data, OutputData::AffectedRows(1)));
    }

    // The region distribution
    let mut distribution = find_region_distribution_by_sql(&cluster, TEST_TABLE_NAME).await;

    let old_distribution = distribution.clone();

    // Selecting target of region migration.
    let region_migration_manager = cluster.metasrv.region_migration_manager();
    let (from_peer_id, from_regions) = distribution.pop_first().unwrap();
    info!(
        "Selecting from peer: {from_peer_id}, and regions: {:?}",
        from_regions
    );
    let (to_peer_id, to_regions) = distribution.pop_first().unwrap();
    info!(
        "Selecting to peer: {to_peer_id}, and regions: {:?}",
        to_regions
    );

    let region_id = RegionId::new(table_id, from_regions.leader_regions[0]);
    // Trigger region migration.
    let procedure_id =
        trigger_migration_by_sql(&cluster, region_id.as_u64(), from_peer_id, to_peer_id).await;

    info!("Started region procedure: {}!", procedure_id);

    // Waits condition by checking procedure state
    let frontend = cluster.fe_instance().clone();
    let procedure_id_for_closure = procedure_id.clone();
    wait_condition(
        Duration::from_secs(10),
        Box::pin(async move {
            loop {
                let state = query_procedure_by_sql(&frontend, &procedure_id_for_closure).await;
                if state == "{\"status\":\"Done\"}" {
                    info!("Migration done: {state}");
                    break;
                } else {
                    info!("Migration not finished: {state}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }),
    )
    .await;

    check_region_migration_events_system_table(
        cluster.fe_instance(),
        &procedure_id,
        region_id.as_u64(),
        from_peer_id,
        to_peer_id,
    )
    .await;

    // Inserts more table.
    let results = insert_values(cluster.fe_instance(), logical_timer).await;
    for result in results {
        assert!(matches!(result.unwrap().data, OutputData::AffectedRows(1)));
    }

    // Asserts the writes.
    assert_values(cluster.fe_instance()).await;

    // Triggers again.
    let err = region_migration_manager
        .submit_procedure(RegionMigrationProcedureTask::new(
            region_id,
            peer_factory(from_peer_id),
            peer_factory(to_peer_id),
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Manual,
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, error::Error::RegionMigrated { .. }));

    let new_distribution = find_region_distribution_by_sql(&cluster, TEST_TABLE_NAME).await;

    assert_ne!(old_distribution, new_distribution);
}

/// A region migration test for a region server contains multiple regions of the table.
pub async fn test_region_migration_multiple_regions(
    store_type: StorageType,
    endpoints: Vec<String>,
) {
    let cluster_name = "test_region_migration_multiple_regions";
    let peer_factory = |id| Peer {
        id,
        addr: PEER_PLACEHOLDER_ADDR.to_string(),
    };

    // Prepares test cluster.
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_region_migration_multiple_regions_data_home");
    let datanodes = 5u64;
    let builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    let const_selector = Arc::new(ConstNodeSelector::new(vec![
        peer_factory(1),
        peer_factory(2),
        peer_factory(2),
    ]));
    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
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
        }))
        .with_shared_home_dir(Arc::new(home_dir))
        .with_meta_selector(const_selector.clone())
        .build(true)
        .await;
    let mut logical_timer = 1685508715000;
    let table_metadata_manager = cluster.metasrv.table_metadata_manager().clone();

    // Prepares test table.
    let table_id = prepare_testing_table(&cluster).await;

    // Inserts data
    let results = insert_values(cluster.fe_instance(), logical_timer).await;
    logical_timer += 1000;
    for result in results {
        assert!(matches!(result.unwrap().data, OutputData::AffectedRows(1)));
    }

    // The region distribution
    let mut distribution = find_region_distribution(&table_metadata_manager, table_id).await;
    assert_eq!(distribution.len(), 2);

    // Selecting target of region migration.
    let region_migration_manager = cluster.metasrv.region_migration_manager();
    let (peer_1, peer_1_regions) = distribution.pop_first().unwrap();
    let (peer_2, peer_2_regions) = distribution.pop_first().unwrap();

    // Picks the peer only contains as from peer.
    let ((from_peer_id, from_regions), (to_peer_id, mut to_regions)) =
        if peer_1_regions.leader_regions.len() == 1 {
            ((peer_1, peer_1_regions), (peer_2, peer_2_regions))
        } else {
            ((peer_2, peer_2_regions), (peer_1, peer_1_regions))
        };

    info!(
        "Selecting from peer: {from_peer_id}, and regions: {:?}",
        from_regions
    );
    info!(
        "Selecting to peer: {to_peer_id}, and regions: {:?}",
        to_regions
    );

    let region_id = RegionId::new(table_id, from_regions.leader_regions[0]);
    // Trigger region migration.
    let procedure = region_migration_manager
        .submit_procedure(RegionMigrationProcedureTask::new(
            region_id,
            peer_factory(from_peer_id),
            peer_factory(to_peer_id),
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Manual,
        ))
        .await
        .unwrap();
    info!("Started region procedure: {}!", procedure.unwrap());

    // Prepares expected region distribution.
    to_regions
        .leader_regions
        .extend(from_regions.leader_regions);
    to_regions
        .follower_regions
        .extend(from_regions.follower_regions);
    // Keeps asc order.
    to_regions.sort();
    distribution.insert(to_peer_id, to_regions);

    // Waits condition
    wait_condition(
        Duration::from_secs(10),
        Box::pin(async move {
            loop {
                let region_migration =
                    find_region_distribution(&table_metadata_manager, table_id).await;
                if region_migration == distribution {
                    info!("Found new distribution: {region_migration:?}");
                    break;
                } else {
                    info!("Found the unexpected distribution: {region_migration:?}, expected: {distribution:?}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }),
    )
    .await;

    // Inserts more table.
    let results = insert_values(cluster.fe_instance(), logical_timer).await;
    for result in results {
        assert!(matches!(result.unwrap().data, OutputData::AffectedRows(1)));
    }

    check_region_migration_events_system_table(
        cluster.fe_instance(),
        &procedure.unwrap().to_string(),
        region_id.as_u64(),
        from_peer_id,
        to_peer_id,
    )
    .await;

    // Asserts the writes.
    assert_values(cluster.fe_instance()).await;

    // Triggers again.
    let err = region_migration_manager
        .submit_procedure(RegionMigrationProcedureTask::new(
            region_id,
            peer_factory(from_peer_id),
            peer_factory(to_peer_id),
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Manual,
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, error::Error::RegionMigrated { .. }));
}

/// A region migration test for a region server contains all regions of the table.
pub async fn test_region_migration_all_regions(store_type: StorageType, endpoints: Vec<String>) {
    let cluster_name = "test_region_migration_all_regions";
    let peer_factory = |id| Peer {
        id,
        addr: PEER_PLACEHOLDER_ADDR.to_string(),
    };

    // Prepares test cluster.
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_region_migration_all_regions_data_home");
    let datanodes = 5u64;
    let builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    let const_selector = Arc::new(ConstNodeSelector::new(vec![
        peer_factory(2),
        peer_factory(2),
        peer_factory(2),
    ]));
    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
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
        }))
        .with_shared_home_dir(Arc::new(home_dir))
        .with_meta_selector(const_selector.clone())
        .build(true)
        .await;
    let mut logical_timer = 1685508715000;
    let table_metadata_manager = cluster.metasrv.table_metadata_manager().clone();

    // Prepares test table.
    let table_id = prepare_testing_table(&cluster).await;

    // Inserts data
    let results = insert_values(cluster.fe_instance(), logical_timer).await;
    logical_timer += 1000;
    for result in results {
        assert!(matches!(result.unwrap().data, OutputData::AffectedRows(1)));
    }

    // The region distribution
    let mut distribution = find_region_distribution(&table_metadata_manager, table_id).await;
    assert_eq!(distribution.len(), 1);

    // Selecting target of region migration.
    let region_migration_manager = cluster.metasrv.region_migration_manager();
    let (from_peer_id, mut from_regions) = distribution.pop_first().unwrap();
    let to_peer_id = 1;
    let mut to_regions = RegionRoleSet::default();
    info!(
        "Selecting from peer: {from_peer_id}, and regions: {:?}",
        from_regions
    );
    info!(
        "Selecting to peer: {to_peer_id}, and regions: {:?}",
        to_regions
    );

    let region_id = RegionId::new(table_id, from_regions.leader_regions[0]);
    // Trigger region migration.
    let procedure = region_migration_manager
        .submit_procedure(RegionMigrationProcedureTask::new(
            region_id,
            peer_factory(from_peer_id),
            peer_factory(to_peer_id),
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Manual,
        ))
        .await
        .unwrap();
    info!("Started region procedure: {}!", procedure.unwrap());

    // Prepares expected region distribution.
    to_regions
        .leader_regions
        .push(from_regions.leader_regions.remove(0));
    // Keeps asc order.
    to_regions.sort();
    distribution.insert(to_peer_id, to_regions);
    distribution.insert(from_peer_id, from_regions);

    // Waits condition
    wait_condition(
        Duration::from_secs(10),
        Box::pin(async move {
            loop {
                let region_migration =
                    find_region_distribution(&table_metadata_manager, table_id).await;
                if region_migration == distribution {
                    info!("Found new distribution: {region_migration:?}");
                    break;
                } else {
                    info!("Found the unexpected distribution: {region_migration:?}, expected: {distribution:?}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }),
    )
    .await;

    check_region_migration_events_system_table(
        cluster.fe_instance(),
        &procedure.unwrap().to_string(),
        region_id.as_u64(),
        from_peer_id,
        to_peer_id,
    )
    .await;

    // Inserts more table.
    let results = insert_values(cluster.fe_instance(), logical_timer).await;
    for result in results {
        assert!(matches!(result.unwrap().data, OutputData::AffectedRows(1)));
    }

    // Asserts the writes.
    assert_values(cluster.fe_instance()).await;

    // Triggers again.
    let err = region_migration_manager
        .submit_procedure(RegionMigrationProcedureTask::new(
            region_id,
            peer_factory(from_peer_id),
            peer_factory(to_peer_id),
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Manual,
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, error::Error::RegionMigrated { .. }));
}

pub async fn test_region_migration_incorrect_from_peer(
    store_type: StorageType,
    endpoints: Vec<String>,
) {
    let cluster_name = "test_region_migration_incorrect_from_peer";
    let peer_factory = |id| Peer {
        id,
        addr: PEER_PLACEHOLDER_ADDR.to_string(),
    };

    // Prepares test cluster.
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_region_migration_incorrect_from_peer_data_home");
    let datanodes = 5u64;
    let builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    let const_selector = Arc::new(ConstNodeSelector::new(vec![
        peer_factory(1),
        peer_factory(2),
        peer_factory(3),
    ]));
    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
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
        }))
        .with_shared_home_dir(Arc::new(home_dir))
        .with_meta_selector(const_selector.clone())
        .build(true)
        .await;
    let logical_timer = 1685508715000;
    let table_metadata_manager = cluster.metasrv.table_metadata_manager().clone();

    // Prepares test table.
    let table_id = prepare_testing_table(&cluster).await;

    // Inserts data
    let results = insert_values(cluster.fe_instance(), logical_timer).await;
    for result in results {
        assert!(matches!(result.unwrap().data, OutputData::AffectedRows(1)));
    }

    // The region distribution
    let distribution = find_region_distribution(&table_metadata_manager, table_id).await;
    assert_eq!(distribution.len(), 3);
    let region_migration_manager = cluster.metasrv.region_migration_manager();

    let region_id = RegionId::new(table_id, 1);

    // Trigger region migration.
    let err = region_migration_manager
        .submit_procedure(RegionMigrationProcedureTask::new(
            region_id,
            peer_factory(5),
            peer_factory(1),
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Manual,
        ))
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        meta_srv::error::Error::LeaderPeerChanged { .. }
    ));
}

pub async fn test_region_migration_incorrect_region_id(
    store_type: StorageType,
    endpoints: Vec<String>,
) {
    let cluster_name = "test_region_migration_incorrect_region_id";
    let peer_factory = |id| Peer {
        id,
        addr: PEER_PLACEHOLDER_ADDR.to_string(),
    };

    // Prepares test cluster.
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_region_migration_incorrect_region_id_data_home");
    let datanodes = 5u64;
    let builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    let const_selector = Arc::new(ConstNodeSelector::new(vec![
        peer_factory(1),
        peer_factory(2),
        peer_factory(3),
    ]));
    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
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
        }))
        .with_shared_home_dir(Arc::new(home_dir))
        .with_meta_selector(const_selector.clone())
        .build(true)
        .await;
    let logical_timer = 1685508715000;
    let table_metadata_manager = cluster.metasrv.table_metadata_manager().clone();

    // Prepares test table.
    let table_id = prepare_testing_table(&cluster).await;

    // Inserts data
    let results = insert_values(cluster.fe_instance(), logical_timer).await;
    for result in results {
        assert!(matches!(result.unwrap().data, OutputData::AffectedRows(1)));
    }

    // The region distribution
    let distribution = find_region_distribution(&table_metadata_manager, table_id).await;
    assert_eq!(distribution.len(), 3);
    let region_migration_manager = cluster.metasrv.region_migration_manager();

    let region_id = RegionId::new(table_id, 5);

    // Trigger region migration.
    let err = region_migration_manager
        .submit_procedure(RegionMigrationProcedureTask::new(
            region_id,
            peer_factory(2),
            peer_factory(1),
            Duration::from_millis(1000),
            RegionMigrationTriggerReason::Manual,
        ))
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        meta_srv::error::Error::RegionRouteNotFound { .. }
    ));
}

struct ConstNodeSelector {
    peers: Vec<Peer>,
}

impl ConstNodeSelector {
    fn new(peers: Vec<Peer>) -> Self {
        Self { peers }
    }
}

#[async_trait::async_trait]
impl Selector for ConstNodeSelector {
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(
        &self,
        _ctx: &Self::Context,
        _opts: SelectorOptions,
    ) -> MetaResult<Self::Output> {
        Ok(self.peers.clone())
    }
}

async fn wait_condition(timeout: Duration, condition: BoxFuture<'static, ()>) {
    tokio::time::timeout(timeout, condition).await.unwrap();
}

async fn assert_values(instance: &Arc<Instance>) {
    let query_ctx = QueryContext::arc();

    let result = instance
        .do_query(
            &format!("select * from {TEST_TABLE_NAME} order by i, ts"),
            query_ctx,
        )
        .await
        .remove(0);

    let expected = "\
+----+---------------------+
| i  | ts                  |
+----+---------------------+
| 5  | 2023-05-31T04:51:55 |
| 5  | 2023-05-31T04:51:56 |
| 15 | 2023-05-31T04:51:55 |
| 15 | 2023-05-31T04:51:56 |
| 55 | 2023-05-31T04:51:55 |
| 55 | 2023-05-31T04:51:56 |
+----+---------------------+";
    check_output_stream(result.unwrap().data, expected).await;
}

async fn prepare_testing_metric_table(cluster: &GreptimeDbCluster) -> TableId {
    let sql = r#"CREATE TABLE phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "");"#;
    let mut result = cluster
        .frontend
        .instance
        .do_query(sql, QueryContext::arc())
        .await;
    let output = result.remove(0).unwrap();
    assert!(matches!(output.data, OutputData::AffectedRows(0)));

    let sql = r#"CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");"#;
    let mut result = cluster
        .frontend
        .instance
        .do_query(sql, QueryContext::arc())
        .await;
    let output = result.remove(0).unwrap();
    assert!(matches!(output.data, OutputData::AffectedRows(0)));

    let sql = r#"CREATE TABLE t2 (ts timestamp time index, job string primary key, val double) engine = metric with ("on_physical_table" = "phy");"#;
    let mut result = cluster
        .frontend
        .instance
        .do_query(sql, QueryContext::arc())
        .await;
    let output = result.remove(0).unwrap();
    assert!(matches!(output.data, OutputData::AffectedRows(0)));

    let table = cluster
        .frontend
        .instance
        .catalog_manager()
        .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "phy", None)
        .await
        .unwrap()
        .unwrap();
    table.table_info().table_id()
}

async fn prepare_testing_table(cluster: &GreptimeDbCluster) -> TableId {
    let sql = format!(
        r"
    CREATE TABLE {TEST_TABLE_NAME} (
        i INT PRIMARY KEY,
        ts TIMESTAMP TIME INDEX,
    ) PARTITION ON COLUMNS (i) (
        i <= 10,
        i > 10 AND i <= 50,
        i > 50
    )"
    );
    let mut result = cluster
        .frontend
        .instance
        .do_query(&sql, QueryContext::arc())
        .await;
    let output = result.remove(0).unwrap();
    assert!(matches!(output.data, OutputData::AffectedRows(0)));

    let table = cluster
        .frontend
        .instance
        .catalog_manager()
        .table(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            TEST_TABLE_NAME,
            None,
        )
        .await
        .unwrap()
        .unwrap();
    table.table_info().table_id()
}

async fn find_region_distribution(
    table_metadata_manager: &TableMetadataManagerRef,
    table_id: TableId,
) -> RegionDistribution {
    table_metadata_manager
        .table_route_manager()
        .get_region_distribution(table_id)
        .await
        .unwrap()
        .unwrap()
}

/// Find region distribution by SQL query
async fn find_region_distribution_by_sql(
    cluster: &GreptimeDbCluster,
    table: &str,
) -> RegionDistribution {
    let query_ctx = QueryContext::arc();

    let OutputData::Stream(stream) = run_sql(
        cluster.fe_instance(),
        &format!(
            r#"select b.peer_id as datanode_id,
                           a.greptime_partition_id as region_id
                    from information_schema.partitions a left join information_schema.region_peers b
                    on a.greptime_partition_id = b.region_id
                    where a.table_name='{table}' order by datanode_id asc"#
        ),
        query_ctx.clone(),
    )
    .await
    .unwrap()
    .data
    else {
        unreachable!();
    };

    let recordbatches = RecordBatches::try_collect(stream).await.unwrap();

    info!("SQL result:\n {}", recordbatches.pretty_print().unwrap());

    let mut distribution = RegionDistribution::new();

    for batch in recordbatches.take() {
        let column = batch.column_by_name("datanode_id").unwrap();
        let datanode_ids = column.as_primitive::<UInt64Type>();
        let column = batch.column_by_name("region_id").unwrap();
        let region_ids = column.as_primitive::<UInt64Type>();

        for (datanode_id, region_id) in datanode_ids.iter().zip(region_ids.iter()) {
            let (Some(datanode_id), Some(region_id)) = (datanode_id, region_id) else {
                unreachable!();
            };

            let region_id = RegionId::from_u64(region_id);
            distribution
                .entry(datanode_id)
                .or_default()
                .add_leader_region(region_id.region_number());
        }
    }

    distribution
}

/// Trigger the region migration by SQL, returns the procedure id if success.
async fn trigger_migration_by_sql(
    cluster: &GreptimeDbCluster,
    region_id: u64,
    from_peer_id: u64,
    to_peer_id: u64,
) -> String {
    let OutputData::RecordBatches(recordbatches) = run_sql(
        cluster.fe_instance(),
        &format!("admin migrate_region({region_id}, {from_peer_id}, {to_peer_id})"),
        QueryContext::arc(),
    )
    .await
    .unwrap()
    .data
    else {
        unreachable!();
    };

    info!("SQL result:\n {}", recordbatches.pretty_print().unwrap());

    let record_batch = &recordbatches.take()[0];
    let column = record_batch.column(0);
    let column = column.as_string::<i32>();
    column.value(0).to_string()
}

/// Query procedure state by SQL.
async fn query_procedure_by_sql(instance: &Arc<Instance>, pid: &str) -> String {
    let OutputData::RecordBatches(recordbatches) = run_sql(
        instance,
        &format!("admin procedure_state('{pid}')"),
        QueryContext::arc(),
    )
    .await
    .unwrap()
    .data
    else {
        unreachable!();
    };

    info!("SQL result:\n {}", recordbatches.pretty_print().unwrap());

    let record_batch = &recordbatches.take()[0];
    let column = record_batch.column(0);
    let column = column.as_string::<i32>();
    column.value(0).to_string()
}

async fn insert_values(instance: &Arc<Instance>, ts: u64) -> Vec<FrontendResult<Output>> {
    let query_ctx = QueryContext::arc();

    let mut results = Vec::new();
    for range in [5, 15, 55] {
        let result = run_sql(
            instance,
            &format!("INSERT INTO {TEST_TABLE_NAME} VALUES ({},{})", range, ts),
            query_ctx.clone(),
        )
        .await;
        results.push(result);
    }

    results
}

async fn run_sql(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> FrontendResult<Output> {
    info!("Run SQL: {sql}");
    instance.do_query(sql, query_ctx).await.remove(0)
}

enum RegionMigrationEvents {
    ProcedureId,
    Timestamp,
    ProcedureState,
    Schema,
    Table,
    EventType,
    RegionMigrationTriggerReason,
    RegionId,
    SrcNodeId,
    DstNodeId,
}

impl Iden for RegionMigrationEvents {
    fn unquoted(&self, s: &mut dyn std::fmt::Write) {
        write!(
            s,
            "{}",
            match self {
                Self::ProcedureId => EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME,
                Self::Timestamp => EVENTS_TABLE_TIMESTAMP_COLUMN_NAME,
                Self::ProcedureState => EVENTS_TABLE_PROCEDURE_STATE_COLUMN_NAME,
                Self::Schema => DEFAULT_PRIVATE_SCHEMA_NAME,
                Self::Table => DEFAULT_EVENTS_TABLE_NAME,
                Self::EventType => EVENTS_TABLE_TYPE_COLUMN_NAME,
                Self::RegionMigrationTriggerReason =>
                    EVENTS_TABLE_REGION_MIGRATION_TRIGGER_REASON_COLUMN_NAME,
                Self::RegionId => EVENTS_TABLE_REGION_ID_COLUMN_NAME,
                Self::SrcNodeId => EVENTS_TABLE_SRC_NODE_ID_COLUMN_NAME,
                Self::DstNodeId => EVENTS_TABLE_DST_NODE_ID_COLUMN_NAME,
            }
        )
        .unwrap();
    }
}

async fn check_region_migration_events_system_table(
    fe_instance: &Arc<Instance>,
    procedure_id: &str,
    region_id: u64,
    from_peer_id: u64,
    to_peer_id: u64,
) {
    // Sleep enough time to ensure the event is recorded.
    tokio::time::sleep(DEFAULT_FLUSH_INTERVAL_SECONDS * 2).await;

    // The query is equivalent to the following SQL:
    //   SELECT region_migration_trigger_reason, procedure_state FROM greptime_private.events WHERE
    //       type = 'region_migration' AND
    //       procedure_id = '${procedure_id}' AND
    //       table_id = ${table_id} AND
    //       region_id = ${region_id} AND
    //       region_migration_src_node_id = ${from_peer_id} AND
    //       region_migration_dst_node_id = ${to_peer_id}
    //       ORDER BY timestamp ASC
    let query = Query::select()
        .column(RegionMigrationEvents::RegionMigrationTriggerReason)
        .column(RegionMigrationEvents::ProcedureState)
        .from((RegionMigrationEvents::Schema, RegionMigrationEvents::Table))
        .and_where(Expr::col(RegionMigrationEvents::EventType).eq(REGION_MIGRATION_EVENT_TYPE))
        .and_where(Expr::col(RegionMigrationEvents::ProcedureId).eq(procedure_id))
        .and_where(Expr::col(RegionMigrationEvents::RegionId).eq(region_id))
        .and_where(Expr::col(RegionMigrationEvents::SrcNodeId).eq(from_peer_id))
        .and_where(Expr::col(RegionMigrationEvents::DstNodeId).eq(to_peer_id))
        .order_by(RegionMigrationEvents::Timestamp, Order::Asc)
        .to_string(PostgresQueryBuilder);

    let result = fe_instance
        .do_query(&query, QueryContext::arc())
        .await
        .remove(0);

    let expected = "\
+---------------------------------+-----------------+
| region_migration_trigger_reason | procedure_state |
+---------------------------------+-----------------+
| Manual                          | Running         |
| Manual                          | Done            |
+---------------------------------+-----------------+";
    check_output_stream(result.unwrap().data, expected).await;
}
