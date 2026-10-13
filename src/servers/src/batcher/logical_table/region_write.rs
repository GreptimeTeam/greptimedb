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

use api::v1::region::{
    BulkInsertRequest, RegionRequest, RegionRequestHeader, bulk_insert_request, region_request,
};
use api::v1::{ArrowIpc, PartitionExprVersion};
use arrow::compute;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_grpc::error::Error as GrpcError;
use common_grpc::flight;
use common_telemetry::tracing_context::TracingContext;
use operator::write_dispatcher::WriteDispatcherRef;
use partition::manager::PartitionRuleManagerRef;
use partition::partition::PartitionRuleRef;
use snafu::ResultExt;
use store_api::storage::RegionId;
use table::metadata::{TableId, TableInfo, TableInfoRef};

use crate::error;
use crate::error::{Error, Result};
use crate::metrics;

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

/// Loads the physical partition rule and its expression versions.
#[async_trait]
pub trait PhysicalFlushPartitionProvider: Send + Sync {
    /// Loads a routing snapshot for the physical table.
    async fn find_table_partition_rule(
        &self,
        table_info: &TableInfo,
    ) -> partition::error::Result<(PartitionRuleRef, HashMap<u32, Option<u64>>)>;
}

/// Dispatches the region requests produced by a physical flush.
#[async_trait]
pub trait PhysicalFlushWriteDispatcher: Send + Sync {
    /// Dispatches one physical flush together, sharing routing refreshes across its writes.
    async fn dispatch(&self, requests: Vec<RegionRequest>) -> Result<usize>;
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
    ) -> partition::error::Result<(PartitionRuleRef, HashMap<u32, Option<u64>>)> {
        self.partition_manager
            .find_table_partition_rule(table_info)
            .await
    }
}

/// Sends a physical flush through the shared write dispatcher.
#[derive(Clone)]
pub(crate) struct WriteDispatcherPhysicalFlushAdapter {
    pub(crate) write_dispatcher: WriteDispatcherRef,
}

#[async_trait]
impl PhysicalFlushWriteDispatcher for WriteDispatcherPhysicalFlushAdapter {
    async fn dispatch(&self, requests: Vec<RegionRequest>) -> Result<usize> {
        self.write_dispatcher
            .dispatch_with_metrics(
                requests,
                metrics::PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                    .with_label_values(&["flush_write_region"]),
            )
            .await
            .context(error::CommonMetaSnafu)
            .map(|response| response.affected_rows)
    }
}

/// Arrow rows selected for one physical region.
pub(crate) struct PlannedRegionBatch {
    pub(crate) region_id: RegionId,
    pub(crate) batch: RecordBatch,
}

#[cfg(test)]
impl PlannedRegionBatch {
    pub(crate) fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

pub(in crate::batcher::logical_table) fn split_combined_batch_by_region(
    combined_batch: &RecordBatch,
    partition_rule: &dyn partition::partition::PartitionRule,
) -> Result<HashMap<u32, partition::partition::RegionMask>> {
    let _timer = metrics::PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
        .with_label_values(&["flush_physical_split_record_batch"])
        .start_timer();
    let map = partition_rule.split_record_batch(combined_batch)?;
    Ok(map)
}

/// Selects the rows assigned to one physical region.
pub(crate) fn plan_region_batch(
    combined_batch: &RecordBatch,
    physical_table_id: TableId,
    region_number: u32,
    mask: &partition::partition::RegionMask,
) -> Result<Option<PlannedRegionBatch>> {
    if mask.select_none() {
        return Ok(None);
    }

    let region_batch = if mask.select_all() {
        combined_batch.clone()
    } else {
        let _timer = metrics::PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_physical_filter_record_batch"])
            .start_timer();
        compute::filter_record_batch(combined_batch, mask.array()).context(error::ArrowSnafu)?
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

/// Partitions a physical flush while retaining columns needed for rerouting.
pub(crate) fn plan_region_batches(
    combined_batch: RecordBatch,
    physical_table_id: TableId,
    partition_rule: &dyn partition::partition::PartitionRule,
) -> Result<Vec<PlannedRegionBatch>> {
    let region_masks = split_combined_batch_by_region(&combined_batch, partition_rule)?;

    // Retain partition columns in the RPC payload so routing can be rebuilt after repartition.
    // Mito's sparse bulk conversion projects the stored fields and encoded primary key.
    let mut planned_batches = Vec::new();
    for (region_number, mask) in region_masks {
        if let Some(planned_batch) =
            plan_region_batch(&combined_batch, physical_table_id, region_number, &mask)?
        {
            planned_batches.push(planned_batch);
        }
    }

    Ok(planned_batches)
}

/// Encodes each region batch with the partition version used to select its rows.
pub(crate) fn encode_region_write_requests(
    planned_batches: Vec<PlannedRegionBatch>,
    skip_wal: bool,
    partition_versions: &HashMap<u32, Option<u64>>,
) -> Result<Vec<RegionRequest>> {
    let mut region_writes = Vec::with_capacity(planned_batches.len());
    for planned in planned_batches {
        let region_id = planned.region_id;
        let (schema_bytes, data_header, payload) = {
            let _timer = metrics::PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_physical_encode_ipc"])
                .start_timer();
            flight::record_batch_to_ipc(planned.batch).map_err(map_ipc_error)?
        };

        let request = RegionRequest {
            header: Some(RegionRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                ..Default::default()
            }),
            body: Some(region_request::Body::BulkInsert(BulkInsertRequest {
                skip_wal,
                region_id: region_id.as_u64(),
                partition_expr_version: partition_versions
                    .get(&region_id.region_number())
                    .copied()
                    .flatten()
                    .map(|value| PartitionExprVersion { value }),
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

        region_writes.push(request);
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
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use api::v1::PartitionExprVersion;
    use api::v1::region::{bulk_insert_request, region_request};
    use arrow::array::{Array, BinaryArray, BooleanArray, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use common_grpc::flight::FlightDecoder;
    use partition::partition::{PartitionRule, RegionMask};
    use store_api::storage::RegionId;

    use crate::batcher::logical_table::batch_convert::{self, RecordBatchWithTsIdx, TableBatch};
    use crate::batcher::logical_table::region_write::{
        PlannedRegionBatch, encode_region_write_requests, plan_region_batches,
    };

    #[test]
    fn test_plan_region_batches_preserves_partition_columns_through_encoding() {
        let logical_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new(
                    "greptime_timestamp",
                    ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("greptime_value", ArrowDataType::Float64, true),
                Field::new("host", ArrowDataType::Utf8, true),
                Field::new("zone", ArrowDataType::Utf8, true),
            ])),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1000_i64, 2000_i64])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0_f64, 2.0_f64])),
                Arc::new(StringArray::from(vec!["node-1", "node-2"])),
                Arc::new(StringArray::from(vec!["east", "west"])),
            ],
        )
        .unwrap();
        let table_batches = [TableBatch {
            table_name: "metric".to_string(),
            table_id: 2048,
            batches: vec![RecordBatchWithTsIdx::try_new(logical_batch, 0).unwrap()],
            row_count: 2,
        }];
        let modified = batch_convert::transform_logical_batches_to_physical(
            &table_batches,
            &HashMap::from([("host".to_string(), 1), ("zone".to_string(), 2)]),
            &HashSet::from(["host"]),
        )
        .unwrap();
        let combined_batch = batch_convert::concat_modified_batches(&modified).unwrap();
        assert!(combined_batch.column_by_name("host").is_some());
        assert!(combined_batch.column_by_name("zone").is_none());
        let expected = combined_batch.clone();
        let mut planned_batches = plan_region_batches(
            combined_batch,
            1024,
            &TwoRegionPartitionRule {
                partition_columns: vec!["host".to_string()],
            },
        )
        .unwrap();
        planned_batches.sort_by_key(|planned| planned.region_id.region_number());

        assert_eq!(2, planned_batches.len());
        assert_eq!(RegionId::new(1024, 1), planned_batches[0].region_id);
        assert_eq!(1, planned_batches[0].num_rows());
        assert_eq!(4, planned_batches[0].batch.num_columns());
        assert_eq!(RegionId::new(1024, 2), planned_batches[1].region_id);
        assert_eq!(1, planned_batches[1].num_rows());
        assert_eq!(4, planned_batches[1].batch.num_columns());

        let writes = encode_region_write_requests(
            planned_batches,
            false,
            &HashMap::from([(1, Some(7)), (2, Some(8))]),
        )
        .unwrap();
        for (index, write) in writes.into_iter().enumerate() {
            let Some(region_request::Body::BulkInsert(request)) = write.body else {
                panic!("expected bulk")
            };
            assert_eq!(
                request.partition_expr_version.unwrap().value,
                7 + index as u64
            );
            let Some(bulk_insert_request::Body::ArrowIpc(ipc)) = request.body else {
                panic!("expected IPC")
            };
            let batch = FlightDecoder::try_from_schema_bytes(&ipc.schema)
                .unwrap()
                .try_decode_record_batch(&ipc.data_header, &ipc.payload)
                .unwrap();
            // The partition-only Arrow projection must retain the complete sparse key,
            // including the zone tag that a later partition rule can require.
            assert!(batch.column_by_name("zone").is_none());
            assert_eq!(batch, expected.slice(index, 1));
            let tags = batch
                .column_by_name("host")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(tags.len(), 1);
            assert_eq!(tags.value(0), if index == 0 { "node-1" } else { "node-2" });
        }
    }

    #[test]
    fn test_encode_region_write_requests_builds_bulk_insert_requests() {
        check_encode_region_write_requests(false);
        check_encode_region_write_requests(true);
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

    fn check_encode_region_write_requests(skip_wal: bool) {
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
        let writes = encode_region_write_requests(
            vec![planned_batch],
            skip_wal,
            &HashMap::from([(1, Some(7))]),
        )
        .unwrap();

        assert_eq!(1, writes.len());
        let Some(region_request::Body::BulkInsert(request)) = &writes[0].body else {
            panic!("expected bulk insert request");
        };
        assert_eq!(RegionId::new(1024, 1).as_u64(), request.region_id);
        assert_eq!(skip_wal, request.skip_wal);
        assert_eq!(
            Some(PartitionExprVersion { value: 7 }),
            request.partition_expr_version
        );
    }
}
