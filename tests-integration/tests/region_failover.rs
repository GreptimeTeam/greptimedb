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

use catalog::kvbackend::{CachedMetaKvBackend, KvBackendCatalogManager};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::key::table_route::TableRouteKey;
use common_meta::key::{RegionDistribution, TableMetaKey};
use common_meta::peer::Peer;
use common_meta::{distributed_time_constants, RegionIdent};
use common_procedure::{watcher, ProcedureWithId};
use common_query::Output;
use common_telemetry::info;
use frontend::error::Result as FrontendResult;
use frontend::instance::Instance;
use futures::TryStreamExt;
use meta_srv::error::Result as MetaResult;
use meta_srv::metasrv::{SelectorContext, SelectorRef};
use meta_srv::procedure::region_failover::{RegionFailoverContext, RegionFailoverProcedure};
use meta_srv::selector::{Namespace, Selector, SelectorOptions};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use table::metadata::TableId;
use tests_integration::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use tests_integration::test_util::{check_output_stream, get_test_store_config, StorageType};
use tokio::time;

#[macro_export]
macro_rules! region_failover_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<integration_region_failover_ $service:lower _test>] {
                $(
                    #[tokio::test(flavor = "multi_thread")]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() {
                        let store_type = tests_integration::test_util::StorageType::$service;
                        if store_type.test_on() {
                            let _ = $crate::region_failover::$test(store_type).await;
                        }

                    }
                )*
            }
        }
    };
}

#[macro_export]
macro_rules! region_failover_tests {
    ($($service:ident),*) => {
        $(
            region_failover_test!(
                $service,

                test_region_failover,
            );
        )*
    };
}

pub async fn test_region_failover(store_type: StorageType) {
    if store_type == StorageType::File {
        // Region failover doesn't make sense when using local file storage.
        return;
    }
    common_telemetry::init_default_ut_logging();
    info!("Running region failover test for {}", store_type);

    let mut logical_timer = 1685508715000;

    let cluster_name = "test_region_failover";

    let (store_config, _guard) = get_test_store_config(&store_type);

    let datanodes = 5u64;
    let builder = GreptimeDbClusterBuilder::new(cluster_name).await;
    let cluster = builder
        .with_datanodes(datanodes as u32)
        .with_store_config(store_config)
        .build()
        .await;

    let frontend = cluster.frontend.clone();

    let table_id = prepare_testing_table(&cluster).await;

    let results = insert_values(&frontend, logical_timer).await;
    logical_timer += 1000;
    for result in results {
        assert!(matches!(result.unwrap(), Output::AffectedRows(1)));
    }

    assert!(has_route_cache(&frontend, table_id).await);

    let distribution = find_region_distribution(&cluster, table_id).await;
    info!("Find region distribution: {distribution:?}");

    let mut foreign = 0;
    for dn in 1..=datanodes {
        if !&distribution.contains_key(&dn) {
            foreign = dn
        }
    }

    let selector = Arc::new(ForeignNodeSelector {
        foreign: Peer {
            id: foreign,
            // "127.0.0.1:3001" is just a placeholder, does not actually connect to it.
            addr: "127.0.0.1:3001".to_string(),
        },
    });

    let failed_region = choose_failed_region(distribution);
    info!("Simulating failed region: {failed_region:#?}");

    run_region_failover_procedure(&cluster, failed_region.clone(), selector).await;

    let distribution = find_region_distribution(&cluster, table_id).await;
    info!("Find region distribution again: {distribution:?}");

    // Waits for invalidating table cache
    time::sleep(Duration::from_millis(100)).await;

    assert!(!has_route_cache(&frontend, table_id).await);

    // Inserts data to each datanode after failover
    let frontend = cluster.frontend.clone();
    let results = insert_values(&frontend, logical_timer).await;
    for result in results {
        assert!(matches!(result.unwrap(), Output::AffectedRows(1)));
    }

    assert_values(&frontend).await;

    assert!(!distribution.contains_key(&failed_region.datanode_id));

    let mut success = false;
    let values = distribution.values();
    for val in values {
        success = success || val.contains(&failed_region.region_number);
    }
    assert!(success)
}

async fn has_route_cache(instance: &Arc<Instance>, table_id: TableId) -> bool {
    let catalog_manager = instance
        .catalog_manager()
        .as_any()
        .downcast_ref::<KvBackendCatalogManager>()
        .unwrap();

    let kv_backend = catalog_manager.table_metadata_manager_ref().kv_backend();

    let cache = kv_backend
        .as_any()
        .downcast_ref::<CachedMetaKvBackend>()
        .unwrap()
        .cache();

    cache
        .get(TableRouteKey::new(table_id).as_raw_key().as_slice())
        .await
        .is_some()
}

async fn insert_values(instance: &Arc<Instance>, ts: u64) -> Vec<FrontendResult<Output>> {
    let query_ctx = QueryContext::arc();

    let mut results = Vec::new();
    for range in [5, 15, 25, 55] {
        let result = insert_value(
            instance,
            &format!("INSERT INTO my_table VALUES ({},{})", range, ts),
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

async fn assert_values(instance: &Arc<Instance>) {
    let query_ctx = QueryContext::arc();

    let result = instance
        .do_query("select * from my_table order by i, ts", query_ctx)
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
| 25 | 2023-05-31T04:51:55 |
| 25 | 2023-05-31T04:51:56 |
| 55 | 2023-05-31T04:51:55 |
| 55 | 2023-05-31T04:51:56 |
+----+---------------------+";
    check_output_stream(result.unwrap(), expected).await;
}

async fn prepare_testing_table(cluster: &GreptimeDbCluster) -> TableId {
    let sql = r"
CREATE TABLE my_table (
    i INT PRIMARY KEY,
    ts TIMESTAMP TIME INDEX,
) PARTITION BY RANGE COLUMNS (i) (
    PARTITION r0 VALUES LESS THAN (10),
    PARTITION r1 VALUES LESS THAN (20),
    PARTITION r2 VALUES LESS THAN (50),
    PARTITION r3 VALUES LESS THAN (MAXVALUE),
)";
    let result = cluster.frontend.do_query(sql, QueryContext::arc()).await;
    result.first().unwrap().as_ref().unwrap();

    let table = cluster
        .frontend
        .catalog_manager()
        .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "my_table")
        .await
        .unwrap()
        .unwrap();
    table.table_info().table_id()
}

async fn find_region_distribution(
    cluster: &GreptimeDbCluster,
    table_id: TableId,
) -> RegionDistribution {
    let manager = cluster.meta_srv.table_metadata_manager();
    let region_distribution = manager
        .table_route_manager()
        .get_region_distribution(table_id)
        .await
        .unwrap()
        .unwrap();

    // test DatanodeTableValues match the table region distribution
    for datanode_id in cluster.datanode_instances.keys() {
        let mut actual = manager
            .datanode_table_manager()
            .tables(*datanode_id)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|x| {
                if x.table_id == table_id {
                    Some(x.regions)
                } else {
                    None
                }
            })
            .flatten()
            .collect::<Vec<_>>();
        actual.sort();

        if let Some(mut expected) = region_distribution.get(datanode_id).cloned() {
            expected.sort();
            assert_eq!(expected, actual);
        } else {
            assert!(actual.is_empty());
        }
    }

    region_distribution
}

fn choose_failed_region(distribution: RegionDistribution) -> RegionIdent {
    let (failed_datanode, failed_region) = distribution
        .iter()
        .filter_map(|(datanode_id, regions)| {
            if !regions.is_empty() {
                Some((*datanode_id, regions[0]))
            } else {
                None
            }
        })
        .next()
        .unwrap();
    RegionIdent {
        cluster_id: 1000,
        datanode_id: failed_datanode,
        table_id: 1025,
        region_number: failed_region,
        engine: "mito2".to_string(),
    }
}

// The "foreign" means the Datanode is not containing any regions to the table before.
pub struct ForeignNodeSelector {
    pub foreign: Peer,
}

#[async_trait::async_trait]
impl Selector for ForeignNodeSelector {
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(
        &self,
        _ns: Namespace,
        _ctx: &Self::Context,
        _opts: SelectorOptions,
    ) -> MetaResult<Self::Output> {
        Ok(vec![self.foreign.clone()])
    }
}

async fn run_region_failover_procedure(
    cluster: &GreptimeDbCluster,
    failed_region: RegionIdent,
    selector: SelectorRef,
) {
    let meta_srv = &cluster.meta_srv;
    let procedure_manager = meta_srv.procedure_manager();
    let procedure = RegionFailoverProcedure::new(
        failed_region.clone(),
        RegionFailoverContext {
            region_lease_secs: 10,
            in_memory: meta_srv.in_memory().clone(),
            mailbox: meta_srv.mailbox().clone(),
            selector,
            selector_ctx: SelectorContext {
                datanode_lease_secs: distributed_time_constants::REGION_LEASE_SECS,
                server_addr: meta_srv.options().server_addr.clone(),
                kv_backend: meta_srv.kv_backend().clone(),
                meta_peer_client: meta_srv.meta_peer_client().clone(),
                table_id: None,
            },
            dist_lock: meta_srv.lock().clone(),
            table_metadata_manager: meta_srv.table_metadata_manager().clone(),
        },
    );
    let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
    let procedure_id = procedure_with_id.id;
    info!("Starting region failover procedure {procedure_id} for region {failed_region:?}");

    let watcher = &mut procedure_manager.submit(procedure_with_id).await.unwrap();
    watcher::wait(watcher).await.unwrap();
}
