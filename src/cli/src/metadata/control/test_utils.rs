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

use common_meta::ddl::test_util::test_create_physical_table_task;
use common_meta::key::table_route::PhysicalTableRouteValue;
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::rpc::store::PutRequest;
use store_api::storage::{RegionId, TableId};
use table::metadata::TableInfo;

/// Puts a key-value pair into the kv backend.
pub async fn put_key(kv_backend: &KvBackendRef, key: &str, value: &str) {
    let put_req = PutRequest::new()
        .with_key(key.as_bytes())
        .with_value(value.as_bytes());
    kv_backend.put(put_req).await.unwrap();
}

/// Prepares the physical table metadata for testing.
///
/// Returns the table info and the table route.
pub async fn prepare_physical_table_metadata(
    table_name: &str,
    table_id: TableId,
) -> (TableInfo, PhysicalTableRouteValue) {
    let mut create_physical_table_task = test_create_physical_table_task(table_name);
    let table_route = PhysicalTableRouteValue::new(vec![RegionRoute {
        region: Region {
            id: RegionId::new(table_id, 1),
            ..Default::default()
        },
        leader_peer: Some(Peer::empty(1)),
        ..Default::default()
    }]);
    create_physical_table_task.set_table_id(table_id);

    (create_physical_table_task.table_info, table_route)
}
