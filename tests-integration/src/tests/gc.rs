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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use common_meta::key::TableMetadataManagerRef;
use common_procedure::ProcedureWithId;
use common_telemetry::info;
use common_test_util::recordbatch::check_output_stream;
use futures::TryStreamExt as _;
use itertools::Itertools;
use meta_srv::gc::{BatchGcProcedure, GcSchedulerOptions, Region2Peers};
use mito2::gc::GcConfig;
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::cluster::GreptimeDbClusterBuilder;
use crate::test_util::{StorageType, TempDirGuard, execute_sql, get_test_store_config};
use crate::tests::test_util::{MockInstanceBuilder, TestContext, wait_procedure};

mod basic;
mod delay_layer;
mod delay_query;
mod race;

/// Helper function to get table route information for GC procedure
pub(crate) async fn get_table_route(
    table_metadata_manager: &TableMetadataManagerRef,
    table_id: TableId,
) -> (Region2Peers, Vec<RegionId>) {
    // Get physical table route
    let (_, physical_table_route) = table_metadata_manager
        .table_route_manager()
        .get_physical_table_route(table_id)
        .await
        .unwrap();

    let mut region_routes = Region2Peers::new();
    let mut regions = Vec::new();

    // Convert region routes to Region2Peers format
    for region_route in physical_table_route.region_routes {
        let region_id = region_route.region.id;
        let leader_peer = region_route.leader_peer.clone().unwrap();
        let follower_peers = region_route.follower_peers.clone();

        region_routes.insert(region_id, (leader_peer, follower_peers));
        regions.push(region_id);
    }

    (region_routes, regions)
}

/// Helper function to list all SST files
pub(crate) async fn list_sst_files(test_context: &TestContext) -> HashSet<String> {
    let mut sst_files = HashSet::new();

    for datanode in test_context.datanodes().values() {
        let region_server = datanode.region_server();
        let mito = region_server.mito_engine().unwrap();
        let all_files = mito
            .all_ssts_from_storage()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.file_path)
            .collect_vec();
        sst_files.extend(all_files);
    }

    sst_files
}
