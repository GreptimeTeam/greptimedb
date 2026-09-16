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

use api::region::RegionResponse;
use api::v1::flow::{DirtyWindowRequests, FlowRequest, FlowResponse};
use api::v1::meta::Peer;
use api::v1::region::{InsertRequests, RegionRequest};
use arrow::array::{StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_meta::cache::{TableFlownodeSetCacheRef, new_table_flownode_set_cache};
use common_meta::error::Result as MetaResult;
use common_meta::instruction::{CacheIdent, CreateFlow};
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::node_manager::{
    DatanodeManager, DatanodeRef, Flownode, FlownodeManager, FlownodeRef,
};
use moka::future::CacheBuilder;
use snafu::ResultExt;
use table::metadata::TableId;
use tokio::sync::mpsc;

use crate::batcher::logical_table::batch_convert::RecordBatchWithTsIdx;
use crate::batcher::logical_table::region_write::PhysicalFlushNodeRequester;
use crate::error;

pub(in crate::batcher::logical_table) fn mock_tag_batch(
    tag_name: &str,
    tag_value: &str,
    ts: i64,
    val: f64,
) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(
            "greptime_timestamp",
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("greptime_value", ArrowDataType::Float64, true),
        Field::new(tag_name, ArrowDataType::Utf8, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![ts])),
            Arc::new(arrow::array::Float64Array::from(vec![val])),
            Arc::new(StringArray::from(vec![tag_value])),
        ],
    )
    .unwrap()
}

pub(in crate::batcher::logical_table) fn mock_aligned_tag_batch(
    tag_name: &str,
    tag_value: &str,
    ts: i64,
    val: f64,
) -> RecordBatchWithTsIdx {
    RecordBatchWithTsIdx::try_new(mock_tag_batch(tag_name, tag_value, ts, val), 0).unwrap()
}

#[derive(Clone)]
pub(in crate::batcher::logical_table) struct ConcurrentMockNodeManager {
    pub(in crate::batcher::logical_table) datanodes: Arc<HashMap<u64, DatanodeRef>>,
}

#[async_trait]
impl DatanodeManager for ConcurrentMockNodeManager {
    async fn datanode(&self, node: &Peer) -> DatanodeRef {
        self.datanodes
            .get(&node.id)
            .expect("datanode not found")
            .clone()
    }
}

pub(in crate::batcher::logical_table) struct NoopFlownode;

#[async_trait]
impl Flownode for NoopFlownode {
    async fn handle(&self, _request: FlowRequest) -> MetaResult<FlowResponse> {
        unimplemented!()
    }

    async fn handle_inserts(&self, _request: InsertRequests) -> MetaResult<FlowResponse> {
        unimplemented!()
    }

    async fn handle_mark_window_dirty(
        &self,
        _req: DirtyWindowRequests,
    ) -> MetaResult<FlowResponse> {
        unimplemented!()
    }
}

#[async_trait]
impl FlownodeManager for ConcurrentMockNodeManager {
    async fn flownode(&self, _node: &Peer) -> FlownodeRef {
        Arc::new(NoopFlownode)
    }
}

pub(in crate::batcher::logical_table) struct RecordingFlownode {
    pub(in crate::batcher::logical_table) requests_tx: mpsc::UnboundedSender<DirtyWindowRequests>,
}

#[async_trait]
impl Flownode for RecordingFlownode {
    async fn handle(&self, _request: FlowRequest) -> MetaResult<FlowResponse> {
        unimplemented!()
    }

    async fn handle_inserts(&self, _request: InsertRequests) -> MetaResult<FlowResponse> {
        unimplemented!()
    }

    async fn handle_mark_window_dirty(&self, req: DirtyWindowRequests) -> MetaResult<FlowResponse> {
        self.requests_tx.send(req).unwrap();
        Ok(FlowResponse::default())
    }
}

pub(in crate::batcher::logical_table) struct FlowNotificationMockNodeManager {
    pub(in crate::batcher::logical_table) flownode: FlownodeRef,
}

#[async_trait]
impl DatanodeManager for FlowNotificationMockNodeManager {
    async fn datanode(&self, _node: &Peer) -> DatanodeRef {
        unimplemented!()
    }
}

#[async_trait]
impl FlownodeManager for FlowNotificationMockNodeManager {
    async fn flownode(&self, _node: &Peer) -> FlownodeRef {
        self.flownode.clone()
    }
}

pub(in crate::batcher::logical_table) async fn mock_table_flownode_cache(
    table_id: TableId,
    peer: Peer,
) -> TableFlownodeSetCacheRef {
    let cache = Arc::new(new_table_flownode_set_cache(
        "test".to_string(),
        CacheBuilder::new(1).build(),
        Arc::new(MemoryKvBackend::default()),
    ));
    cache
        .invalidate(&[CacheIdent::CreateFlow(CreateFlow {
            flow_id: 1,
            source_table_ids: vec![table_id],
            partition_to_peer_mapping: vec![(0, peer.clone()), (1, peer)],
        })])
        .await
        .unwrap();
    cache
}

#[async_trait]
impl PhysicalFlushNodeRequester for ConcurrentMockNodeManager {
    async fn handle(&self, peer: &Peer, request: RegionRequest) -> error::Result<RegionResponse> {
        let datanode = self.datanode(peer).await;
        datanode
            .handle(request)
            .await
            .context(error::CommonMetaSnafu)
    }
}
