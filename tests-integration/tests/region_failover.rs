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

use catalog::helper::TableGlobalKey;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE};
use common_meta::RegionIdent;
use common_procedure::{watcher, ProcedureWithId};
use common_telemetry::info;
use meta_srv::metasrv::SelectorContext;
use meta_srv::procedure::region_failover::{RegionFailoverContext, RegionFailoverProcedure};
use meta_srv::table_routes;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use tests_integration::test_util::{get_test_store_config, StorageType};

// TODO(LFC): wait for close regions in datanode, and read/write route in frontend ready
#[tokio::test(flavor = "multi_thread")]
async fn test_region_failover() {
    common_telemetry::init_default_ut_logging();

    let cluster_name = "test_region_failover";

    let (store_config, _guard) = get_test_store_config(&StorageType::File, cluster_name);

    let cluster = GreptimeDbClusterBuilder::new(cluster_name)
        .with_datanodes(2)
        .with_store_config(store_config)
        .build()
        .await;

    prepare_testing_table(&cluster).await;

    let distribution = find_region_distribution(&cluster).await;
    info!("Find region distribution: {distribution:?}");

    let failed_region = choose_failed_region(distribution);
    info!("Simulating failed region: {failed_region:#?}");

    run_region_failover_procedure(&cluster, failed_region.clone()).await;

    let mut distribution = find_region_distribution(&cluster).await;
    info!("Find region distribution again: {distribution:?}");

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
        table_id: 1025,
        engine: MITO_ENGINE.to_string(),
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table: "my_table".to_string(),
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
