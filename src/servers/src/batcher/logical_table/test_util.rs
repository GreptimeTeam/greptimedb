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
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use api::v1::ColumnSchema;
use api::v1::flow::{DirtyWindowRequests, FlowRequest, FlowResponse};
use api::v1::meta::Peer;
use api::v1::region::InsertRequests;
use arrow::array::{StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use catalog::memory::MemoryCatalogManager;
use common_meta::error::Result as MetaResult;
use common_meta::node_manager::{
    DatanodeManager, DatanodeRef, Flownode, FlownodeManager, FlownodeRef,
};
use common_meta::test_util::MockDatanodeManager;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_time::timestamp::TimeUnit;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema as DtColumnSchema, Schema as DtSchema};
use operator::test_util::{create_partition_rule_manager, prepare_mocked_backend};
use operator::write_dispatcher::WriteDispatcher;
use session::context::{QueryContext, QueryContextRef};
use store_api::metric_engine_consts::{LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME};
use table::dist_table::DistTable;
use table::test_util::table_info::test_table_info;
use tokio::sync::mpsc;

use crate::batcher::logical_table::batch_convert::RecordBatchWithTsIdx;
pub(crate) use crate::batcher::logical_table::pending_worker::WorkerCommand;
use crate::batcher::logical_table::{
    LogicalTablePendingRowsBatcher, PendingRowsSchemaAlterer, batch_key_from_ctx,
};
use crate::batcher::test_util::mock_table_flownode_cache;
use crate::error;

struct UnexpectedSchemaAlterer;

#[async_trait]
impl PendingRowsSchemaAlterer for UnexpectedSchemaAlterer {
    async fn create_tables_if_missing_batch(
        &self,
        _: &str,
        _: &str,
        _: &[(&str, &[ColumnSchema])],
        _: bool,
        _: QueryContextRef,
    ) -> error::Result<()> {
        unreachable!()
    }

    async fn add_missing_prom_tag_columns_batch(
        &self,
        _: &str,
        _: &str,
        _: &[(&str, &[String])],
        _: QueryContextRef,
    ) -> error::Result<()> {
        unreachable!()
    }
}

pub(crate) async fn submission_batcher(
    sync: bool,
) -> (
    Arc<LogicalTablePendingRowsBatcher>,
    mpsc::Receiver<WorkerCommand>,
) {
    let schema = DtSchema::try_new(vec![
        DtColumnSchema::new(
            "greptime_timestamp",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        DtColumnSchema::new("greptime_value", ConcreteDataType::float64_datatype(), true),
    ])
    .unwrap();
    let mut table = test_table_info(1, "cpu", "public", "greptime", Arc::new(schema));
    table.meta.engine = METRIC_ENGINE_NAME.into();
    table.meta.options.extra_options.insert(
        LOGICAL_TABLE_METADATA_KEY.into(),
        GREPTIME_PHYSICAL_TABLE.into(),
    );
    let catalog = MemoryCatalogManager::new_with_table(DistTable::table(Arc::new(table)));
    let partitions = create_partition_rule_manager(prepare_mocked_backend().await).await;
    let nodes = Arc::new(MockDatanodeManager::new(()));
    let dispatcher = Arc::new(WriteDispatcher::new(nodes.clone(), partitions.clone()));
    let mut batcher = LogicalTablePendingRowsBatcher::try_new(
        partitions,
        nodes,
        dispatcher,
        catalog,
        mock_table_flownode_cache(1, vec![]).await,
        true,
        Arc::new(UnexpectedSchemaAlterer),
        TimeUnit::Millisecond,
        Duration::from_secs(1),
        2,
        1,
        1,
        1,
        NonZeroUsize::new(1).unwrap(),
    )
    .unwrap();
    Arc::get_mut(&mut batcher).unwrap().pending_rows_batch_sync = sync;
    let (_, receiver) = batcher
        .workers
        .get_or_create(batch_key_from_ctx(&QueryContext::arc()), 1)
        .await;
    (batcher, receiver.unwrap())
}

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
