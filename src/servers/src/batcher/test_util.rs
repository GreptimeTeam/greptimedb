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

use common_meta::cache::{TableFlownodeSetCacheRef, new_table_flownode_set_cache};
use common_meta::instruction::{CacheIdent, CreateFlow};
use common_meta::key::FlowPartitionId;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::peer::Peer;
use moka::future::CacheBuilder;
use store_api::storage::TableId;

/// Creates an isolated Flow target cache, preserving duplicate peers across partitions.
pub(in crate::batcher) async fn mock_table_flownode_cache(
    table_id: TableId,
    partition_to_peer_mapping: Vec<(FlowPartitionId, Peer)>,
) -> TableFlownodeSetCacheRef {
    let cache = Arc::new(new_table_flownode_set_cache(
        "batcher-test".to_string(),
        CacheBuilder::new(4).build(),
        Arc::new(MemoryKvBackend::default()),
    ));
    if !partition_to_peer_mapping.is_empty() {
        cache
            .invalidate(&[CacheIdent::CreateFlow(CreateFlow {
                flow_id: 1,
                source_table_ids: vec![table_id],
                partition_to_peer_mapping,
            })])
            .await
            .unwrap();
    }
    cache
}
