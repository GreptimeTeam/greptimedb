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
use std::sync::{Arc, Mutex};

use api::region::RegionResponse;
use api::v1::region::{RegionRequest, bulk_insert_request, region_request};
use common_grpc::flight::FlightDecoder;
use common_meta::cache::{TableFlownodeSetCacheRef, new_table_flownode_set_cache};
use common_meta::error::{Result as MetaResult, UnexpectedSnafu};
use common_meta::instruction::{CacheIdent, CreateFlow};
use common_meta::key::FlowPartitionId;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::peer::Peer;
use common_meta::test_util::MockDatanodeHandler;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use moka::future::CacheBuilder;
use store_api::storage::{RegionId, TableId};

/// Exercises real bulk dispatch with region failures and malformed acknowledgements.
#[derive(Clone, Default)]
pub(crate) struct RecordingBulkHandler {
    pub failed_regions: Vec<u32>,
    pub short_regions: Vec<u32>,
    pub attempts: Arc<Mutex<HashMap<u32, usize>>>,
}

#[async_trait::async_trait]
impl MockDatanodeHandler for RecordingBulkHandler {
    async fn handle(&self, _: &Peer, request: RegionRequest) -> MetaResult<RegionResponse> {
        let Some(region_request::Body::BulkInsert(request)) = request.body else {
            panic!("expected bulk insert")
        };
        let region = RegionId::from_u64(request.region_id).region_number();
        *self.attempts.lock().unwrap().entry(region).or_default() += 1;
        if self.failed_regions.contains(&region) {
            return UnexpectedSnafu {
                err_msg: format!("injected bulk failure in region {region}"),
            }
            .fail();
        }
        let Some(bulk_insert_request::Body::ArrowIpc(ipc)) = request.body else {
            panic!("expected Arrow IPC")
        };
        let batch = FlightDecoder::try_from_schema_bytes(&ipc.schema)
            .unwrap()
            .try_decode_record_batch(&ipc.data_header, &ipc.payload)
            .unwrap();
        Ok(RegionResponse::new(
            batch.num_rows() - usize::from(self.short_regions.contains(&region)),
        ))
    }

    async fn handle_query(
        &self,
        _: &Peer,
        _: QueryRequest,
    ) -> MetaResult<SendableRecordBatchStream> {
        unreachable!("bulk dispatch must not query storage")
    }
}

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
