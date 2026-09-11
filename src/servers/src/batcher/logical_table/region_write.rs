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

use api::v1::ArrowIpc;
use api::v1::meta::Peer;
use api::v1::region::{
    BulkInsertRequest, RegionRequest, RegionRequestHeader, bulk_insert_request, region_request,
};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_grpc::error::Error as GrpcError;
use common_grpc::flight::record_batch_to_ipc;
use common_meta::node_manager::NodeManagerRef;
use common_telemetry::tracing_context::TracingContext;
use partition::manager::PartitionRuleManagerRef;
use partition::partition::PartitionRuleRef;
use snafu::ResultExt;
use store_api::storage::RegionId;
use table::metadata::{TableId, TableInfo, TableInfoRef};

use crate::batcher::logical_table::batch_convert::strip_partition_columns_from_batch;
use crate::error;
use crate::error::{Error, Result};
use crate::metrics::PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED;

#[derive(Clone)]
pub struct PhysicalTableMetadata {
    pub table_info: TableInfoRef,
    /// Mapping from column name to column id
    pub col_name_to_ids: Option<HashMap<String, u32>>,
}

#[async_trait]
pub trait PhysicalFlushCatalogProvider: Send + Sync {
    async fn physical_table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
        query_ctx: &session::context::QueryContext,
    ) -> catalog::error::Result<Option<PhysicalTableMetadata>>;
}

#[async_trait]
pub trait PhysicalFlushPartitionProvider: Send + Sync {
    async fn find_table_partition_rule(
        &self,
        table_info: &TableInfo,
    ) -> partition::error::Result<PartitionRuleRef>;

    async fn find_region_leader(&self, region_id: RegionId) -> Result<Peer>;
}

#[async_trait]
pub trait PhysicalFlushNodeRequester: Send + Sync {
    async fn handle(
        &self,
        peer: &Peer,
        request: RegionRequest,
    ) -> Result<api::region::RegionResponse>;
}

#[derive(Clone)]
pub(in crate::batcher::logical_table) struct CatalogManagerPhysicalFlushAdapter {
    pub(in crate::batcher::logical_table) catalog_manager: CatalogManagerRef,
}

#[async_trait]
impl PhysicalFlushCatalogProvider for CatalogManagerPhysicalFlushAdapter {
    async fn physical_table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
        query_ctx: &session::context::QueryContext,
    ) -> catalog::error::Result<Option<PhysicalTableMetadata>> {
        self.catalog_manager
            .table(catalog, schema, table_name, Some(query_ctx))
            .await
            .map(|table| {
                table.map(|table| {
                    let table_info = table.table_info();
                    let name_to_ids = table_info.name_to_ids();
                    PhysicalTableMetadata {
                        table_info,
                        col_name_to_ids: name_to_ids,
                    }
                })
            })
    }
}

#[derive(Clone)]
pub(in crate::batcher::logical_table) struct PartitionManagerPhysicalFlushAdapter {
    pub(in crate::batcher::logical_table) partition_manager: PartitionRuleManagerRef,
}

#[async_trait]
impl PhysicalFlushPartitionProvider for PartitionManagerPhysicalFlushAdapter {
    async fn find_table_partition_rule(
        &self,
        table_info: &TableInfo,
    ) -> partition::error::Result<PartitionRuleRef> {
        self.partition_manager
            .find_table_partition_rule(table_info)
            .await
            .map(|(rule, _)| rule)
    }

    async fn find_region_leader(&self, region_id: RegionId) -> Result<Peer> {
        let peer = self.partition_manager.find_region_leader(region_id).await?;
        Ok(peer)
    }
}

#[derive(Clone)]
pub(in crate::batcher::logical_table) struct NodeManagerPhysicalFlushAdapter {
    pub(in crate::batcher::logical_table) node_manager: NodeManagerRef,
}

#[async_trait]
impl PhysicalFlushNodeRequester for NodeManagerPhysicalFlushAdapter {
    async fn handle(
        &self,
        peer: &Peer,
        request: RegionRequest,
    ) -> error::Result<api::region::RegionResponse> {
        let datanode = self.node_manager.datanode(peer).await;
        datanode
            .handle(request)
            .await
            .context(error::CommonMetaSnafu)
    }
}

pub(in crate::batcher::logical_table) struct FlushRegionWrite {
    pub(in crate::batcher::logical_table) datanode: Peer,
    pub(in crate::batcher::logical_table) request: RegionRequest,
}

pub(in crate::batcher::logical_table) struct PlannedRegionBatch {
    pub(in crate::batcher::logical_table) region_id: RegionId,
    pub(in crate::batcher::logical_table) batch: RecordBatch,
}

#[cfg(test)]
impl PlannedRegionBatch {
    pub(in crate::batcher::logical_table) fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

pub(in crate::batcher::logical_table) struct ResolvedRegionBatch {
    pub(in crate::batcher::logical_table) planned: PlannedRegionBatch,
    pub(in crate::batcher::logical_table) datanode: Peer,
}

pub(in crate::batcher::logical_table) fn should_dispatch_concurrently(
    region_write_count: usize,
) -> bool {
    region_write_count > 1
}

pub(in crate::batcher::logical_table) async fn flush_region_writes_concurrently(
    node_manager: &(impl PhysicalFlushNodeRequester + ?Sized),
    writes: Vec<FlushRegionWrite>,
) -> Result<usize> {
    let mut affected_rows = 0;
    if !should_dispatch_concurrently(writes.len()) {
        for write in writes {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_write_region"])
                .start_timer();
            affected_rows += node_manager
                .handle(&write.datanode, write.request)
                .await?
                .affected_rows;
        }
        return Ok(affected_rows);
    }

    let write_futures = writes.into_iter().map(|write| async move {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_write_region"])
            .start_timer();

        let response = node_manager.handle(&write.datanode, write.request).await?;
        Ok::<_, Error>(response.affected_rows)
    });

    // todo(hl): should be bounded.
    let affected_rows = futures::future::try_join_all(write_futures)
        .await?
        .into_iter()
        .sum();
    Ok(affected_rows)
}

pub(in crate::batcher::logical_table) fn split_combined_batch_by_region(
    combined_batch: &RecordBatch,
    partition_rule: &dyn partition::partition::PartitionRule,
) -> Result<HashMap<u32, partition::partition::RegionMask>> {
    let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
        .with_label_values(&["flush_physical_split_record_batch"])
        .start_timer();
    let map = partition_rule.split_record_batch(combined_batch)?;
    Ok(map)
}

pub(in crate::batcher::logical_table) fn prepare_physical_region_routing_batch(
    combined_batch: RecordBatch,
    partition_columns: &[String],
) -> Result<RecordBatch> {
    if partition_columns.is_empty() {
        return Ok(combined_batch);
    }
    strip_partition_columns_from_batch(combined_batch)
}

pub(in crate::batcher::logical_table) fn plan_region_batch(
    stripped_batch: &RecordBatch,
    physical_table_id: TableId,
    region_number: u32,
    mask: &partition::partition::RegionMask,
) -> Result<Option<PlannedRegionBatch>> {
    if mask.select_none() {
        return Ok(None);
    }

    let region_batch = if mask.select_all() {
        stripped_batch.clone()
    } else {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_physical_filter_record_batch"])
            .start_timer();
        filter_record_batch(stripped_batch, mask.array()).context(error::ArrowSnafu)?
    };

    let row_count = region_batch.num_rows();
    if row_count == 0 {
        return Ok(None);
    }

    Ok(Some(PlannedRegionBatch {
        region_id: RegionId::new(physical_table_id, region_number),
        batch: region_batch,
    }))
}

pub(in crate::batcher::logical_table) fn plan_region_batches(
    combined_batch: RecordBatch,
    physical_table_id: TableId,
    partition_rule: &dyn partition::partition::PartitionRule,
    partition_columns: &[String],
) -> Result<Vec<PlannedRegionBatch>> {
    let region_masks = split_combined_batch_by_region(&combined_batch, partition_rule)?;
    let stripped_batch = prepare_physical_region_routing_batch(combined_batch, partition_columns)?;

    let mut planned_batches = Vec::new();
    for (region_number, mask) in region_masks {
        if let Some(planned_batch) =
            plan_region_batch(&stripped_batch, physical_table_id, region_number, &mask)?
        {
            planned_batches.push(planned_batch);
        }
    }

    Ok(planned_batches)
}

pub(in crate::batcher::logical_table) async fn resolve_region_targets(
    planned_batches: Vec<PlannedRegionBatch>,
    partition_manager: &(impl PhysicalFlushPartitionProvider + ?Sized),
) -> Result<Vec<ResolvedRegionBatch>> {
    let mut resolved_batches = Vec::with_capacity(planned_batches.len());
    for planned in planned_batches {
        let datanode = {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_physical_resolve_region_leader"])
                .start_timer();
            partition_manager
                .find_region_leader(planned.region_id)
                .await?
        };

        resolved_batches.push(ResolvedRegionBatch { planned, datanode });
    }

    Ok(resolved_batches)
}

pub(in crate::batcher::logical_table) fn encode_region_write_requests(
    resolved_batches: Vec<ResolvedRegionBatch>,
) -> Result<Vec<FlushRegionWrite>> {
    let mut region_writes = Vec::with_capacity(resolved_batches.len());
    for resolved in resolved_batches {
        let region_id = resolved.planned.region_id;
        let (schema_bytes, data_header, payload) = {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_physical_encode_ipc"])
                .start_timer();
            record_batch_to_ipc(resolved.planned.batch).map_err(map_ipc_error)?
        };

        let request = RegionRequest {
            header: Some(RegionRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                ..Default::default()
            }),
            body: Some(region_request::Body::BulkInsert(BulkInsertRequest {
                region_id: region_id.as_u64(),
                partition_expr_version: None,
                // Set aligned_schema_version to None so that datanode will check the batch schema again to see if any
                // column is missing.
                aligned_schema_version: None,
                body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                    schema: schema_bytes,
                    data_header,
                    payload,
                })),
            })),
        };

        region_writes.push(FlushRegionWrite {
            datanode: resolved.datanode,
            request,
        });
    }

    Ok(region_writes)
}

pub(in crate::batcher::logical_table) fn map_ipc_error(error: GrpcError) -> Error {
    match error {
        GrpcError::NotSupported { feat } => Error::NotSupported { feat },
        GrpcError::InvalidFlightData { reason, .. } => Error::Internal { err_msg: reason },
        error => Error::Internal {
            err_msg: error.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use api::region::RegionResponse;
    use api::v1::meta::Peer;
    use api::v1::region::{RegionRequest, region_request};
    use arrow::array::{BinaryArray, BooleanArray, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use common_meta::error::Result as MetaResult;
    use common_meta::node_manager::{Datanode, DatanodeRef};
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use partition::partition::{PartitionRule, RegionMask};
    use store_api::storage::RegionId;
    use tokio::time::sleep;

    use crate::batcher::logical_table::region_write::{
        FlushRegionWrite, PlannedRegionBatch, ResolvedRegionBatch, encode_region_write_requests,
        flush_region_writes_concurrently, plan_region_batches, should_dispatch_concurrently,
    };
    use crate::batcher::logical_table::test_util::ConcurrentMockNodeManager;

    #[tokio::test]
    async fn test_flush_region_writes_concurrently_dispatches_multiple_datanodes() {
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_inflight = Arc::new(AtomicUsize::new(0));
        let datanode1: DatanodeRef = Arc::new(ConcurrentMockDatanode {
            delay: Duration::from_millis(100),
            inflight: inflight.clone(),
            max_inflight: max_inflight.clone(),
        });
        let datanode2: DatanodeRef = Arc::new(ConcurrentMockDatanode {
            delay: Duration::from_millis(100),
            inflight,
            max_inflight: max_inflight.clone(),
        });

        let mut datanodes = HashMap::new();
        datanodes.insert(1, datanode1);
        datanodes.insert(2, datanode2);
        let node_manager = Arc::new(ConcurrentMockNodeManager {
            datanodes: Arc::new(datanodes),
        });

        let writes = vec![
            FlushRegionWrite {
                datanode: Peer {
                    id: 1,
                    addr: "node1".to_string(),
                },
                request: RegionRequest::default(),
            },
            FlushRegionWrite {
                datanode: Peer {
                    id: 2,
                    addr: "node2".to_string(),
                },
                request: RegionRequest::default(),
            },
        ];

        flush_region_writes_concurrently(node_manager.as_ref(), writes)
            .await
            .unwrap();
        assert!(max_inflight.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn test_should_dispatch_concurrently_by_region_count() {
        assert!(!should_dispatch_concurrently(0));
        assert!(!should_dispatch_concurrently(1));
        assert!(should_dispatch_concurrently(2));
    }

    #[test]
    fn test_plan_region_batches_splits_and_strips_partition_columns() {
        let combined_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("__primary_key", ArrowDataType::Binary, false),
                Field::new(
                    "greptime_timestamp",
                    ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("greptime_value", ArrowDataType::Float64, true),
                Field::new("host", ArrowDataType::Utf8, true),
            ])),
            vec![
                Arc::new(BinaryArray::from(vec![b"k1".as_slice(), b"k2".as_slice()])),
                Arc::new(TimestampMillisecondArray::from(vec![1000_i64, 2000_i64])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0_f64, 2.0_f64])),
                Arc::new(StringArray::from(vec!["node-1", "node-2"])),
            ],
        )
        .unwrap();
        let mut planned_batches = plan_region_batches(
            combined_batch,
            1024,
            &TwoRegionPartitionRule {
                partition_columns: vec!["host".to_string()],
            },
            &["host".to_string()],
        )
        .unwrap();
        planned_batches.sort_by_key(|planned| planned.region_id.region_number());

        assert_eq!(2, planned_batches.len());
        assert_eq!(RegionId::new(1024, 1), planned_batches[0].region_id);
        assert_eq!(1, planned_batches[0].num_rows());
        assert_eq!(3, planned_batches[0].batch.num_columns());
        assert_eq!(RegionId::new(1024, 2), planned_batches[1].region_id);
        assert_eq!(1, planned_batches[1].num_rows());
        assert_eq!(3, planned_batches[1].batch.num_columns());
    }

    #[test]
    fn test_encode_region_write_requests_builds_bulk_insert_requests() {
        let planned_batch = PlannedRegionBatch {
            region_id: RegionId::new(1024, 1),
            batch: RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("__primary_key", ArrowDataType::Binary, false),
                    Field::new(
                        "greptime_timestamp",
                        ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new("greptime_value", ArrowDataType::Float64, true),
                ])),
                vec![
                    Arc::new(BinaryArray::from(vec![b"k1".as_slice()])),
                    Arc::new(TimestampMillisecondArray::from(vec![1000_i64])),
                    Arc::new(arrow::array::Float64Array::from(vec![1.0_f64])),
                ],
            )
            .unwrap(),
        };
        let resolved_batch = ResolvedRegionBatch {
            planned: planned_batch,
            datanode: Peer {
                id: 1,
                addr: "node-1".to_string(),
            },
        };
        let writes = encode_region_write_requests(vec![resolved_batch]).unwrap();

        assert_eq!(1, writes.len());
        assert_eq!(1, writes[0].datanode.id);
        let Some(region_request::Body::BulkInsert(request)) = &writes[0].request.body else {
            panic!("expected bulk insert request");
        };
        assert_eq!(RegionId::new(1024, 1).as_u64(), request.region_id);
    }

    struct TwoRegionPartitionRule {
        partition_columns: Vec<String>,
    }

    impl PartitionRule for TwoRegionPartitionRule {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn partition_columns(&self) -> &[String] {
            &self.partition_columns
        }

        fn find_region(
            &self,
            _values: &[datatypes::prelude::Value],
        ) -> partition::error::Result<store_api::storage::RegionNumber> {
            unimplemented!()
        }

        fn split_record_batch(
            &self,
            _record_batch: &RecordBatch,
        ) -> partition::error::Result<HashMap<store_api::storage::RegionNumber, RegionMask>>
        {
            Ok(HashMap::from([
                (1, RegionMask::new(BooleanArray::from(vec![true, false]), 1)),
                (2, RegionMask::new(BooleanArray::from(vec![false, true]), 1)),
                (
                    3,
                    RegionMask::new(BooleanArray::from(vec![false, false]), 0),
                ),
            ]))
        }
    }

    #[derive(Clone)]
    struct ConcurrentMockDatanode {
        delay: Duration,
        inflight: Arc<AtomicUsize>,
        max_inflight: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Datanode for ConcurrentMockDatanode {
        async fn handle(&self, _request: RegionRequest) -> MetaResult<RegionResponse> {
            let now = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            loop {
                let max = self.max_inflight.load(Ordering::SeqCst);
                if now <= max {
                    break;
                }
                if self
                    .max_inflight
                    .compare_exchange(max, now, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }

            sleep(self.delay).await;
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            Ok(RegionResponse::new(0))
        }

        async fn handle_query(
            &self,
            _request: QueryRequest,
        ) -> MetaResult<SendableRecordBatchStream> {
            unimplemented!()
        }
    }
}
