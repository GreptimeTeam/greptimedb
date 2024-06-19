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

use chrono::DateTime;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE};
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::sequence::SequenceBuilder;
use common_meta::state_store::KvStateStore;
use common_meta::ClusterId;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_time::util as time_util;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, RawSchema};
use table::metadata::{RawTableInfo, RawTableMeta, TableIdent, TableType};
use table::requests::TableOptions;

use crate::cluster::{MetaPeerClientBuilder, MetaPeerClientRef};
use crate::handler::{HeartbeatMailbox, Pushers};
use crate::key::{DatanodeLeaseKey, LeaseValue};
use crate::lock::memory::MemLock;
use crate::metasrv::SelectorContext;
use crate::procedure::region_failover::RegionFailoverManager;
use crate::selector::lease_based::LeaseBasedSelector;

pub(crate) fn new_region_route(region_id: u64, peers: &[Peer], leader_node: u64) -> RegionRoute {
    let region = Region {
        id: region_id.into(),
        ..Default::default()
    };

    let leader_peer = peers.iter().find(|peer| peer.id == leader_node).cloned();

    RegionRoute {
        region,
        leader_peer,
        follower_peers: vec![],
        leader_status: None,
        leader_down_since: None,
    }
}

/// Builds and returns a [`SelectorContext`]. To access its inner state,
/// use `memory_backend` on [`MetaPeerClientRef`].
pub(crate) fn create_selector_context() -> SelectorContext {
    let in_memory = Arc::new(MemoryKvBackend::new());
    let meta_peer_client = MetaPeerClientBuilder::default()
        .election(None)
        .in_memory(in_memory.clone())
        .build()
        .map(Arc::new)
        // Safety: all required fields set at initialization
        .unwrap();

    SelectorContext {
        datanode_lease_secs: 10,
        flownode_lease_secs: 10,
        server_addr: "127.0.0.1:3002".to_string(),
        kv_backend: in_memory,
        meta_peer_client,
        table_id: None,
    }
}

pub(crate) fn create_region_failover_manager() -> Arc<RegionFailoverManager> {
    let kv_backend = Arc::new(MemoryKvBackend::new());

    let pushers = Pushers::default();
    let mailbox_sequence =
        SequenceBuilder::new("test_heartbeat_mailbox", kv_backend.clone()).build();
    let mailbox = HeartbeatMailbox::create(pushers, mailbox_sequence);

    let state_store = Arc::new(KvStateStore::new(kv_backend.clone()));
    let procedure_manager = Arc::new(LocalManager::new(ManagerConfig::default(), state_store));

    let selector = Arc::new(LeaseBasedSelector);
    let selector_ctx = create_selector_context();

    let in_memory = Arc::new(MemoryKvBackend::new());
    Arc::new(RegionFailoverManager::new(
        10,
        in_memory,
        kv_backend.clone(),
        mailbox,
        procedure_manager,
        (selector, selector_ctx),
        Arc::new(MemLock::default()),
        Arc::new(TableMetadataManager::new(kv_backend)),
    ))
}

pub(crate) async fn prepare_table_region_and_info_value(
    table_metadata_manager: &TableMetadataManagerRef,
    table: &str,
) {
    let table_info = RawTableInfo {
        ident: TableIdent::new(1),
        name: table.to_string(),
        desc: None,
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        meta: RawTableMeta {
            schema: RawSchema::new(vec![ColumnSchema::new(
                "a",
                ConcreteDataType::string_datatype(),
                true,
            )]),
            primary_key_indices: vec![],
            value_indices: vec![],
            engine: MITO_ENGINE.to_string(),
            next_column_id: 1,
            region_numbers: vec![1, 2, 3, 4],
            options: TableOptions::default(),
            created_on: DateTime::default(),
            partition_key_indices: vec![],
        },
        table_type: TableType::Base,
    };

    let region_route_factory = |region_id: u64, peer: u64| RegionRoute {
        region: Region {
            id: region_id.into(),
            ..Default::default()
        },
        leader_peer: Some(Peer {
            id: peer,
            addr: String::new(),
        }),
        follower_peers: vec![],
        leader_status: None,
        leader_down_since: None,
    };

    // Region distribution:
    // Datanode => Regions
    // 1 => 1, 2
    // 2 => 3
    // 3 => 4
    let region_routes = vec![
        region_route_factory(1, 1),
        region_route_factory(2, 1),
        region_route_factory(3, 2),
        region_route_factory(4, 3),
    ];
    table_metadata_manager
        .create_table_metadata(
            table_info,
            TableRouteValue::physical(region_routes),
            HashMap::default(),
        )
        .await
        .unwrap();
}

pub(crate) async fn put_datanodes(
    cluster_id: ClusterId,
    meta_peer_client: &MetaPeerClientRef,
    datanodes: Vec<Peer>,
) {
    let backend = meta_peer_client.memory_backend();
    for datanode in datanodes {
        let lease_key = DatanodeLeaseKey {
            cluster_id,
            node_id: datanode.id,
        };
        let lease_value = LeaseValue {
            timestamp_millis: time_util::current_time_millis(),
            node_addr: datanode.addr,
        };
        let lease_key_bytes: Vec<u8> = lease_key.try_into().unwrap();
        let lease_value_bytes: Vec<u8> = lease_value.try_into().unwrap();
        let put_request = common_meta::rpc::store::PutRequest {
            key: lease_key_bytes,
            value: lease_value_bytes,
            ..Default::default()
        };
        backend.put(put_request).await.unwrap();
    }
}
