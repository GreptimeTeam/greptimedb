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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use catalog::helper::TableGlobalKey;
use catalog::remote::{CachedMetaKvBackend, Kv};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE};
use common_meta::instruction::TableIdent;
use common_meta::rpc::router::TableRoute;
use common_meta::table_name::TableName;
use common_meta::RegionIdent;
use common_procedure::{watcher, ProcedureWithId};
use common_query::Output;
use common_telemetry::info;
use frontend::catalog::FrontendCatalogManager;
use frontend::error::Result as FrontendResult;
use frontend::instance::Instance;
use meta_srv::metasrv::SelectorContext;
use meta_srv::procedure::region_failover::{RegionFailoverContext, RegionFailoverProcedure};
use meta_srv::table_routes;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
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
    common_telemetry::init_default_ut_logging();

    let mut logical_timer = 1685508715000;

    let cluster_name = "test_region_failover";

    let (store_config, _guard) = get_test_store_config(&store_type, cluster_name);

    let cluster = GreptimeDbClusterBuilder::new(cluster_name)
        .with_datanodes(2)
        .with_store_config(store_config)
        .build()
        .await;

    let frontend = cluster.frontend.clone();

    prepare_testing_table(&cluster).await;

    let results = write_datas(&frontend, logical_timer).await;
    logical_timer += 1000;
    for result in results {
        assert!(matches!(result.unwrap(), Output::AffectedRows(1)));
    }

    let cache_key = TableGlobalKey {
        catalog_name: "greptime".to_string(),
        schema_name: "public".to_string(),
        table_name: "my_table".to_string(),
    }
    .to_string();

    let table_name = TableName {
        catalog_name: "greptime".to_string(),
        schema_name: "public".to_string(),
        table_name: "my_table".to_string(),
    };

    let cache = get_table_cache(&frontend, &cache_key).unwrap();
    assert!(cache.is_some());
    let route_cache = get_route_cache(&frontend, &table_name);
    assert!(route_cache.is_some());

    let distribution = find_region_distribution(&cluster).await;
    info!("Find region distribution: {distribution:?}");

    let failed_region = choose_failed_region(distribution);
    info!("Simulating failed region: {failed_region:#?}");

    run_region_failover_procedure(&cluster, failed_region.clone()).await;

    let mut distribution = find_region_distribution(&cluster).await;
    info!("Find region distribution again: {distribution:?}");

    // Waits for invalidating table cache
    time::sleep(Duration::from_millis(100)).await;

    let cache = get_table_cache(&frontend, &cache_key);
    assert!(cache.is_none());
    let route_cache = get_route_cache(&frontend, &table_name);
    assert!(route_cache.is_none());

    // Inserts data to each datanode after failover
    let frontend = cluster.frontend.clone();
    let results = write_datas(&frontend, logical_timer).await;
    for result in results {
        assert!(matches!(result.unwrap(), Output::AffectedRows(1)));
    }

    assert_writes(&frontend).await;

    assert!(!distribution
        .remove(&failed_region.datanode_id)
        .unwrap()
        .contains(&failed_region.region_number));
    // Since there are only two datanodes, the other datanode is the candidate.
    assert!(distribution
        .values()
        .next()
        .unwrap()
        .contains(&failed_region.region_number));
}

fn get_table_cache(instance: &Arc<Instance>, key: &str) -> Option<Option<Kv>> {
    let catalog_manager = instance
        .catalog_manager()
        .as_any()
        .downcast_ref::<FrontendCatalogManager>()
        .unwrap();

    let kvbackend = catalog_manager.backend();

    let kvbackend = kvbackend
        .as_any()
        .downcast_ref::<CachedMetaKvBackend>()
        .unwrap();
    let cache = kvbackend.cache();

    cache.get(key.as_bytes())
}

fn get_route_cache(instance: &Arc<Instance>, table_name: &TableName) -> Option<Arc<TableRoute>> {
    let catalog_manager = instance
        .catalog_manager()
        .as_any()
        .downcast_ref::<FrontendCatalogManager>()
        .unwrap();
    let pm = catalog_manager.partition_manager();
    let cache = pm.table_routes().cache();
    cache.get(table_name)
}

async fn write_datas(instance: &Arc<Instance>, ts: u64) -> Vec<FrontendResult<Output>> {
    let query_ctx = QueryContext::arc();

    let mut results = Vec::new();
    for range in [5, 15, 25, 55] {
        let result = write_data(
            instance,
            &format!("INSERT INTO my_table VALUES ({},{})", range, ts),
            query_ctx.clone(),
        )
        .await;
        results.push(result);
    }

    results
}

async fn write_data(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> FrontendResult<Output> {
    instance.do_query(sql, query_ctx).await.remove(0)
}

async fn assert_writes(instance: &Arc<Instance>) {
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

async fn prepare_testing_table(cluster: &GreptimeDbCluster) {
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
    assert!(result[0].is_ok());
}

async fn find_region_distribution(cluster: &GreptimeDbCluster) -> HashMap<u64, Vec<u32>> {
    let key = TableGlobalKey {
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: "my_table".to_string(),
    };
    let value = table_routes::get_table_global_value(&cluster.kv_store, &key)
        .await
        .unwrap()
        .unwrap();
    value.regions_id_map
}

fn choose_failed_region(distribution: HashMap<u64, Vec<u32>>) -> RegionIdent {
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
        table_ident: TableIdent {
            table_id: 1025,
            engine: MITO_ENGINE.to_string(),
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table: "my_table".to_string(),
        },
        region_number: failed_region,
    }
}

async fn run_region_failover_procedure(cluster: &GreptimeDbCluster, failed_region: RegionIdent) {
    let meta_srv = &cluster.meta_srv;
    let procedure_manager = meta_srv.procedure_manager();
    let procedure = RegionFailoverProcedure::new(
        failed_region.clone(),
        RegionFailoverContext {
            mailbox: meta_srv.mailbox(),
            selector: meta_srv.selector(),
            selector_ctx: SelectorContext {
                datanode_lease_secs: meta_srv.options().datanode_lease_secs,
                server_addr: meta_srv.options().server_addr.clone(),
                kv_store: meta_srv.kv_store(),
                catalog: None,
                schema: None,
            },
            dist_lock: meta_srv.lock().clone(),
        },
    );
    let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
    let procedure_id = procedure_with_id.id;
    info!("Starting region failover procedure {procedure_id} for region {failed_region:?}");

    let watcher = &mut procedure_manager.submit(procedure_with_id).await.unwrap();
    watcher::wait(watcher).await.unwrap();
}
