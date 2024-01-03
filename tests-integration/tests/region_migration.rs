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
use common_config::wal::KafkaConfig;
use common_config::WalConfig;
use common_meta::key::{RegionDistribution, TableMetadataManagerRef};
use common_meta::peer::Peer;
use common_meta::wal::kafka::KafkaConfig as MetaKafkaConfig;
use common_meta::wal::WalConfig as MetaWalConfig;
use common_query::Output;
use common_telemetry::info;
use common_test_util::temp_dir::create_temp_dir;
use frontend::error::Result as FrontendResult;
use frontend::instance::Instance;
use futures::future::BoxFuture;
use meta_srv::error::Result as MetaResult;
use meta_srv::metasrv::SelectorContext;
use meta_srv::procedure::region_migration::RegionMigrationProcedureTask;
use meta_srv::selector::{Namespace, Selector, SelectorOptions};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use store_api::storage::RegionId;
use table::metadata::TableId;
use tests_integration::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use tests_integration::test_util::{
    check_output_stream, get_test_store_config, run_test_with_kafka_wal, StorageType,
};
use uuid::Uuid;

const TEST_TABLE_NAME: &str = "migration_target";

#[tokio::test(flavor = "multi_thread")]
async fn test_region_migration_fs() {
    common_telemetry::init_default_ut_logging();
    run_test_with_kafka_wal(|endpoints| {
        Box::pin(async move { test_region_migration(StorageType::File, endpoints).await })
    })
    .await
}

pub async fn test_region_migration(store_type: StorageType, endpoints: Vec<String>) {
    let cluster_name = "test_region_migration";
    let peer_factory = |id| Peer {
        id,
        addr: "127.0.0.1:3001".to_string(),
    };

    // Prepares test cluster.
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_migration_data_home");
    let datanodes = 5u64;
    let builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    let const_selector = Arc::new(ConstNodeSelector {
        peers: vec![peer_factory(1), peer_factory(2), peer_factory(3)],
    });
    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
        .with_wal_config(WalConfig::Kafka(KafkaConfig {
            broker_endpoints: endpoints.clone(),
            linger: Duration::from_millis(25),
            ..Default::default()
        }))
        .with_meta_wal_config(MetaWalConfig::Kafka(MetaKafkaConfig {
            broker_endpoints: endpoints,
            num_topics: 3,
            topic_name_prefix: Uuid::new_v4().to_string(),
            ..Default::default()
        }))
        .with_shared_home_dir(Arc::new(home_dir))
        .with_meta_selector(const_selector.clone())
        .build()
        .await;
    let mut logical_timer = 1685508715000;
    let table_metadata_manager = cluster.meta_srv.table_metadata_manager().clone();

    // Prepares test table.
    let table_id = prepare_testing_table(&cluster).await;

    // Inserts data
    let results = insert_values(&cluster.frontend, logical_timer).await;
    logical_timer += 1000;
    for result in results {
        assert!(matches!(result.unwrap(), Output::AffectedRows(1)));
    }

    // The region distribution
    let mut distribution = find_region_distribution(&table_metadata_manager, table_id).await;

    // Selecting target of region migration.
    let region_migration_manager = cluster.meta_srv.region_migration_manager();
    let (from_peer_id, from_regions) = distribution.pop_first().unwrap();
    info!(
        "Selecting from peer: {from_peer_id}, and regions: {:?}",
        from_regions
    );
    let (to_peer_id, mut to_regions) = distribution.pop_first().unwrap();
    info!(
        "Selecting to peer: {from_peer_id}, and regions: {:?}",
        to_regions
    );

    let region_id = RegionId::new(table_id, from_regions[0]);
    // Trigger region migration.
    let procedure = region_migration_manager
        .submit_procedure(RegionMigrationProcedureTask::new(
            0,
            region_id,
            peer_factory(from_peer_id),
            peer_factory(to_peer_id),
        ))
        .await
        .unwrap();
    info!("Started region procedure: {}!", procedure.unwrap());

    // Prepares expected region distribution.
    to_regions.extend(from_regions);
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
    let results = insert_values(&cluster.frontend, logical_timer).await;
    for result in results {
        assert!(matches!(result.unwrap(), Output::AffectedRows(1)));
    }

    // Asserts the writes.
    assert_values(&cluster.frontend).await;

    // Triggers again.
    let procedure = region_migration_manager
        .submit_procedure(RegionMigrationProcedureTask::new(
            0,
            region_id,
            peer_factory(from_peer_id),
            peer_factory(to_peer_id),
        ))
        .await
        .unwrap();
    assert!(procedure.is_none());
}

pub struct ConstNodeSelector {
    peers: Vec<Peer>,
}

#[async_trait::async_trait]
impl Selector for ConstNodeSelector {
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(
        &self,
        _ns: Namespace,
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
    check_output_stream(result.unwrap(), expected).await;
}

async fn prepare_testing_table(cluster: &GreptimeDbCluster) -> TableId {
    let sql = format!(
        r"
    CREATE TABLE {TEST_TABLE_NAME} (
        i INT PRIMARY KEY,
        ts TIMESTAMP TIME INDEX,
    ) PARTITION BY RANGE COLUMNS (i) (
        PARTITION r0 VALUES LESS THAN (10),
        PARTITION r1 VALUES LESS THAN (50),
        PARTITION r3 VALUES LESS THAN (MAXVALUE),
    )"
    );
    let mut result = cluster.frontend.do_query(&sql, QueryContext::arc()).await;
    let output = result.remove(0).unwrap();
    assert!(matches!(output, Output::AffectedRows(0)));

    let table = cluster
        .frontend
        .catalog_manager()
        .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, TEST_TABLE_NAME)
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

async fn insert_values(instance: &Arc<Instance>, ts: u64) -> Vec<FrontendResult<Output>> {
    let query_ctx = QueryContext::arc();

    let mut results = Vec::new();
    for range in [5, 15, 55] {
        let result = insert_value(
            instance,
            &format!("INSERT INTO {TEST_TABLE_NAME} VALUES ({},{})", range, ts),
            query_ctx.clone(),
        )
        .await;
        results.push(result);
    }

    results
}

async fn insert_value(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> FrontendResult<Output> {
    instance.do_query(sql, query_ctx).await.remove(0)
}
