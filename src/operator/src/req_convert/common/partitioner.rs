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
    BulkInsertRequest, DeleteRequest, DeleteRequests, InsertRequest, InsertRequests, RegionRequest,
    bulk_insert_request, region_request::Body,
};
use api::v1::{ArrowIpc, PartitionExprVersion, Rows};
use common_error::ext::BoxedError;
use common_grpc::flight;
use datatypes::arrow::compute;
use datatypes::arrow::record_batch::RecordBatch;
use partition::PartitionRuleRef;
use partition::manager::PartitionRuleManager;
use partition::splitter::RowSplitter;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, RegionNumber, TableId};
use table::metadata::TableInfo;

use crate::error::{self, Result, SplitDeleteSnafu, SplitInsertSnafu};

/// Loads table routing rules and converts rows to region write requests.
pub struct Partitioner<'a> {
    partition_manager: &'a PartitionRuleManager,
}

/// A single snapshot of a table's partition rule and expression versions.
pub(crate) struct TablePartitioner {
    table_id: TableId,
    rule: PartitionRuleRef,
    versions: HashMap<RegionNumber, Option<PartitionExprVersion>>,
}

impl<'a> Partitioner<'a> {
    /// Creates a partitioner backed by the shared routing caches.
    pub fn new(partition_manager: &'a PartitionRuleManager) -> Self {
        Self { partition_manager }
    }

    /// Routes insert rows using the current table partitions.
    pub async fn partition_insert_requests(
        &self,
        table_info: &TableInfo,
        rows: Rows,
        skip_wal: bool,
    ) -> Result<Vec<InsertRequest>> {
        TablePartitioner::try_new(self.partition_manager, table_info)
            .await
            .context(SplitInsertSnafu)?
            .partition_insert_requests(rows, skip_wal)
    }

    /// Routes delete rows using the current table partitions.
    pub async fn partition_delete_requests(
        &self,
        table_info: &TableInfo,
        rows: Rows,
    ) -> Result<Vec<DeleteRequest>> {
        TablePartitioner::try_new(self.partition_manager, table_info)
            .await
            .context(SplitDeleteSnafu)?
            .partition_delete_requests(rows)
    }
}

impl TablePartitioner {
    /// Reads a table's partition rule and expression versions into one routing snapshot.
    pub async fn try_new(
        partition_manager: &PartitionRuleManager,
        table_info: &TableInfo,
    ) -> partition::error::Result<Self> {
        let (rule, versions) = partition_manager
            .find_table_partition_rule(table_info)
            .await?;
        Ok(Self {
            table_id: table_info.table_id(),
            rule,
            versions: versions
                .into_iter()
                .map(|(region, version)| {
                    (region, version.map(|value| PartitionExprVersion { value }))
                })
                .collect(),
        })
    }

    /// Repartitions a rejected write after the caller refreshes the routing caches.
    ///
    /// Takes ownership so row payloads can move into their new regions. Requests whose
    /// region and partition version still match retain their original payloads.
    pub fn repartition_request(&self, request: RegionRequest) -> Result<Vec<RegionRequest>> {
        let RegionRequest { header, body } = request;
        let bodies = match body.context(error::UnexpectedSnafu {
            violated: "Cannot reroute a request without a body",
        })? {
            Body::Inserts(inserts) => {
                let mut bodies = Vec::new();
                for insert in inserts.requests {
                    let region_id = RegionId::from_u64(insert.region_id);
                    if self.matches_region(region_id, insert.partition_expr_version) {
                        bodies.push(Body::Inserts(InsertRequests {
                            requests: vec![insert],
                        }));
                        continue;
                    }
                    let rows = insert.rows.context(error::UnexpectedSnafu {
                        violated: "Cannot reroute an insert without rows",
                    })?;
                    for request in self.partition_insert_requests(rows, insert.skip_wal)? {
                        bodies.push(Body::Inserts(InsertRequests {
                            requests: vec![request],
                        }));
                    }
                }
                bodies
            }
            Body::Deletes(deletes) => {
                let mut bodies = Vec::new();
                for delete in deletes.requests {
                    let region_id = RegionId::from_u64(delete.region_id);
                    if self.matches_region(region_id, delete.partition_expr_version) {
                        bodies.push(Body::Deletes(DeleteRequests {
                            requests: vec![delete],
                        }));
                        continue;
                    }
                    let rows = delete.rows.context(error::UnexpectedSnafu {
                        violated: "Cannot reroute a delete without rows",
                    })?;
                    for request in self.partition_delete_requests(rows)? {
                        bodies.push(Body::Deletes(DeleteRequests {
                            requests: vec![request],
                        }));
                    }
                }
                bodies
            }
            Body::BulkInsert(bulk) => {
                let region_id = RegionId::from_u64(bulk.region_id);
                if self.matches_region(region_id, bulk.partition_expr_version) {
                    vec![Body::BulkInsert(bulk)]
                } else {
                    let bulk_insert_request::Body::ArrowIpc(ipc) =
                        bulk.body.context(error::UnexpectedSnafu {
                            violated: "Cannot reroute a bulk insert without Arrow IPC data",
                        })?;
                    let batch = flight::FlightDecoder::try_from_schema_bytes(&ipc.schema)
                        .and_then(|mut decoder| {
                            decoder.try_decode_record_batch(&ipc.data_header, &ipc.payload)
                        })
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;
                    self.partition_bulk_insert_requests(batch, Some(ipc), bulk.skip_wal)?
                        .into_iter()
                        .map(Body::BulkInsert)
                        .collect()
                }
            }
            _ => {
                return error::UnexpectedSnafu {
                    violated: "Only writes can be rerouted",
                }
                .fail();
            }
        };
        Ok(bodies
            .into_iter()
            .map(|body| RegionRequest {
                header: header.clone(),
                body: Some(body),
            })
            .collect())
    }

    /// Moves insert rows into requests for this snapshot's destination regions.
    pub fn partition_insert_requests(
        &self,
        rows: Rows,
        skip_wal: bool,
    ) -> Result<Vec<InsertRequest>> {
        Ok(RowSplitter::new(self.rule.clone())
            .split(rows)
            .context(SplitInsertSnafu)?
            .into_iter()
            .map(|(region, rows)| InsertRequest {
                region_id: RegionId::new(self.table_id, region).as_u64(),
                rows: Some(rows),
                partition_expr_version: self.version(region),
                skip_wal,
            })
            .collect())
    }

    /// Moves delete rows into requests for this snapshot's destination regions.
    pub fn partition_delete_requests(&self, rows: Rows) -> Result<Vec<DeleteRequest>> {
        Ok(RowSplitter::new(self.rule.clone())
            .split(rows)
            .context(SplitDeleteSnafu)?
            .into_iter()
            .map(|(region, rows)| DeleteRequest {
                region_id: RegionId::new(self.table_id, region).as_u64(),
                rows: Some(rows),
                partition_expr_version: self.version(region),
            })
            .collect())
    }

    /// Splits an Arrow batch, reusing the original IPC when one region selects all rows.
    pub fn partition_bulk_insert_requests(
        &self,
        batch: RecordBatch,
        mut original_ipc: Option<ArrowIpc>,
        skip_wal: bool,
    ) -> Result<Vec<BulkInsertRequest>> {
        let masks = self
            .rule
            .split_record_batch(&batch)
            .context(SplitInsertSnafu)?;
        let mut requests = Vec::with_capacity(masks.len());
        for (region, mask) in masks {
            if mask.select_none() {
                continue;
            }
            let original = if mask.select_all() {
                original_ipc.take()
            } else {
                None
            };
            let ipc = if let Some(ipc) = original {
                ipc
            } else {
                let selected = if mask.select_all() {
                    batch.clone()
                } else {
                    compute::filter_record_batch(&batch, mask.array())
                        .context(error::ComputeArrowSnafu)?
                };
                let (schema, data_header, payload) = flight::record_batch_to_ipc(selected)
                    .map_err(BoxedError::new)
                    .context(error::ExternalSnafu)?;
                ArrowIpc {
                    schema,
                    data_header,
                    payload,
                }
            };
            requests.push(BulkInsertRequest {
                region_id: RegionId::new(self.table_id, region).as_u64(),
                partition_expr_version: self.version(region),
                // The destination datanode validates against its current schema.
                aligned_schema_version: None,
                skip_wal,
                body: Some(bulk_insert_request::Body::ArrowIpc(ipc)),
            });
        }
        Ok(requests)
    }

    /// Whether an earlier request still matches this partition snapshot.
    fn matches_region(&self, region_id: RegionId, version: Option<PartitionExprVersion>) -> bool {
        region_id.table_id() == self.table_id
            && self.versions.get(&region_id.region_number()) == Some(&version)
    }

    fn version(&self, region: RegionNumber) -> Option<PartitionExprVersion> {
        self.versions.get(&region).copied().flatten()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::region::{
        AlignedSchemaVersion, BulkInsertRequest, DeleteRequest, InsertRequest, RegionRequestHeader,
    };
    use api::v1::value::ValueData;
    use api::v1::{ArrowIpc, PartitionExprVersion};
    use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, Value};
    use common_error::ext::{ErrorExt, RetryHint};
    use common_error::status_code::StatusCode;
    use common_grpc::flight;
    use common_meta::cache::{self, TableInfoCache};
    use common_meta::key::TableMetadataManager;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::test_util::MockKvBackendBuilder;
    use common_meta::rpc::store::RangeResponse;
    use datatypes::arrow::array::Int32Array;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use moka::future::Cache;
    use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};

    use super::*;
    use crate::error::Error;
    use crate::test_util;

    #[tokio::test]
    async fn test_partition_requests_preserve_routing_errors() {
        let table_info = test_util::new_test_table_info(1, "table_1", [1].into_iter());
        for (backend_unavailable, expected_status, expected_hint) in [
            (false, StatusCode::TableNotFound, RetryHint::NonRetryable),
            (true, StatusCode::Internal, RetryHint::Retryable),
        ] {
            let backend = Arc::new(
                MockKvBackendBuilder::default()
                    .range_fn(Arc::new(move |_| {
                        if backend_unavailable {
                            common_meta::error::NoLeaderSnafu { table_id: 1u32 }.fail()
                        } else {
                            Ok(RangeResponse {
                                kvs: Vec::new(),
                                more: false,
                            })
                        }
                    }))
                    .build()
                    .unwrap(),
            );
            let routes = Arc::new(cache::new_table_route_cache(
                "partitioner_test_routes".to_string(),
                Cache::new(16),
                backend.clone(),
            ));
            let partitions = Arc::new(partition::cache::new_partition_info_cache(
                "partitioner_test_partitions".to_string(),
                Cache::new(16),
                routes.clone(),
            ));
            let manager = PartitionRuleManager::new(backend, routes, partitions);
            let partitioner = Partitioner::new(&manager);
            let insert_error = partitioner
                .partition_insert_requests(&table_info, Rows::default(), false)
                .await
                .unwrap_err();
            let delete_error = partitioner
                .partition_delete_requests(&table_info, Rows::default())
                .await
                .unwrap_err();

            assert!(matches!(insert_error, Error::SplitInsert { .. }));
            assert!(matches!(delete_error, Error::SplitDelete { .. }));
            for error in [insert_error, delete_error] {
                assert_eq!(error.status_code(), expected_status);
                assert_eq!(error.retry_hint(), expected_hint);
            }
        }
    }

    async fn create_fixture() -> (PartitionRuleManagerRef, TableInfoCache) {
        let backend = Arc::new(MemoryKvBackend::new());
        let manager = test_util::create_partition_rule_manager(backend.clone()).await;
        TableMetadataManager::new(backend.clone())
            .create_table_metadata(
                test_util::new_test_table_info(42, "logical_reroute", [1, 2, 3].into_iter()),
                TableRouteValue::logical(1),
                Default::default(),
            )
            .await
            .unwrap();
        let cache = cache::new_table_info_cache(
            "reroute_test_table_info".to_string(),
            moka::future::Cache::new(16),
            backend,
        );
        (manager, cache)
    }

    fn build_request(body: Body) -> RegionRequest {
        RegionRequest {
            header: Some(RegionRequestHeader {
                dbname: "greptime.public".to_string(),
                ..Default::default()
            }),
            body: Some(body),
        }
    }

    fn build_rows(values: &[i32]) -> Rows {
        Rows {
            schema: vec![ColumnSchema {
                column_name: "a".to_string(),
                datatype: ColumnDataType::Int32 as i32,
                ..Default::default()
            }],
            rows: values
                .iter()
                .map(|value| Row {
                    values: vec![Value {
                        value_data: Some(ValueData::I32Value(*value)),
                    }],
                })
                .collect(),
        }
    }

    fn build_row_request(
        rows: Rows,
        insert: bool,
        region: u32,
        version: Option<u64>,
    ) -> RegionRequest {
        let region_id = RegionId::new(1, region).as_u64();
        let partition_expr_version = version.map(|value| PartitionExprVersion { value });
        let body = if insert {
            Body::Inserts(InsertRequests {
                requests: vec![InsertRequest {
                    region_id,
                    partition_expr_version,
                    rows: Some(rows),
                    skip_wal: true,
                }],
            })
        } else {
            Body::Deletes(DeleteRequests {
                requests: vec![DeleteRequest {
                    region_id,
                    partition_expr_version,
                    rows: Some(rows),
                }],
            })
        };
        build_request(body)
    }

    fn row_request_parts(request: &RegionRequest) -> (u64, Option<u64>, &Rows) {
        match request.body.as_ref().unwrap() {
            Body::Inserts(inserts) => {
                assert_eq!(inserts.requests.len(), 1);
                let insert = &inserts.requests[0];
                assert!(insert.skip_wal);
                (
                    insert.region_id,
                    insert.partition_expr_version.map(|v| v.value),
                    insert.rows.as_ref().unwrap(),
                )
            }
            Body::Deletes(deletes) => {
                assert_eq!(deletes.requests.len(), 1);
                let delete = &deletes.requests[0];
                (
                    delete.region_id,
                    delete.partition_expr_version.map(|v| v.value),
                    delete.rows.as_ref().unwrap(),
                )
            }
            _ => panic!("expected row write"),
        }
    }

    #[tokio::test]
    async fn test_repartition_rows_updates_partitions_without_losing_rows() {
        let (manager, cache) = create_fixture().await;
        let table_info = cache.get(1).await.unwrap().unwrap();
        let (_, versions) = manager
            .find_table_partition_rule(&table_info)
            .await
            .unwrap();
        for insert in [true, false] {
            let request = build_row_request(build_rows(&[1, 20, 70]), insert, 99, Some(0));
            let header = request.header.clone();
            let rerouted = repartition_with_cached_table(&manager, &cache, request)
                .await
                .unwrap();
            assert_eq!(rerouted.len(), 3);
            let mut actual = Vec::new();
            for request in rerouted {
                assert_eq!(request.header, header);
                let (id, version, rows) = row_request_parts(&request);
                let region_id = RegionId::from_u64(id);
                assert_eq!(region_id.table_id(), 1);
                assert_eq!(version, versions[&region_id.region_number()]);
                assert_eq!(rows.schema, build_rows(&[]).schema);
                assert_eq!(rows.rows.len(), 1);
                actual.push((region_id.region_number(), rows.rows[0].clone()));
            }
            actual.sort_by_key(|(region, _)| *region);
            assert_eq!(
                actual,
                vec![
                    (1, build_rows(&[70]).rows.remove(0)),
                    (2, build_rows(&[20]).rows.remove(0)),
                    (3, build_rows(&[1]).rows.remove(0))
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_unchanged_row_routes_preserve_allocations() {
        let (manager, cache) = create_fixture().await;
        let table_info = cache.get(1).await.unwrap().unwrap();
        let (_, versions) = manager
            .find_table_partition_rule(&table_info)
            .await
            .unwrap();
        for insert in [true, false] {
            let request = build_row_request(build_rows(&[1]), insert, 3, versions[&3]);
            let original_rows = row_request_parts(&request).2;
            let row_ptr = original_rows.rows.as_ptr();
            let value_ptr = original_rows.rows[0].values.as_ptr();
            let rerouted = repartition_with_cached_table(&manager, &cache, request)
                .await
                .unwrap();
            assert_eq!(rerouted.len(), 1);
            let rows = row_request_parts(&rerouted[0]).2;
            assert_eq!(rows.rows.as_ptr(), row_ptr);
            assert_eq!(rows.rows[0].values.as_ptr(), value_ptr);
            assert_eq!(rows, &build_rows(&[1]));
        }
    }

    fn build_bulk_request(region: u32, version: Option<u64>, values: Vec<i32>) -> RegionRequest {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap();
        let (schema, data_header, payload) = flight::record_batch_to_ipc(batch).unwrap();
        build_request(Body::BulkInsert(BulkInsertRequest {
            region_id: RegionId::new(1, region).as_u64(),
            partition_expr_version: version.map(|value| PartitionExprVersion { value }),
            aligned_schema_version: Some(AlignedSchemaVersion {
                schema_version: 123,
            }),
            skip_wal: true,
            body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                schema,
                data_header,
                payload,
            })),
        }))
    }

    #[tokio::test]
    async fn test_repartition_bulk_reencodes_only_changed_partitions() {
        let (manager, cache) = create_fixture().await;
        let table_info = cache.get(1).await.unwrap().unwrap();
        let (_, versions) = manager
            .find_table_partition_rule(&table_info)
            .await
            .unwrap();
        let request = build_bulk_request(99, Some(0), vec![1, 20, 70]);
        let header = request.header.clone();
        let rerouted = repartition_with_cached_table(&manager, &cache, request)
            .await
            .unwrap();
        assert_eq!(rerouted.len(), 3);
        let mut actual = Vec::new();
        for request in rerouted {
            assert_eq!(request.header, header);
            let Some(Body::BulkInsert(bulk)) = request.body else {
                panic!("expected bulk insert")
            };
            let region = RegionId::from_u64(bulk.region_id);
            assert_eq!(region.table_id(), 1);
            assert!(bulk.skip_wal);
            assert_eq!(bulk.aligned_schema_version, None);
            assert_eq!(
                bulk.partition_expr_version.map(|v| v.value),
                versions[&region.region_number()]
            );
            let Some(bulk_insert_request::Body::ArrowIpc(ipc)) = bulk.body else {
                panic!("expected Arrow IPC")
            };
            let batch = flight::FlightDecoder::try_from_schema_bytes(&ipc.schema)
                .unwrap()
                .try_decode_record_batch(&ipc.data_header, &ipc.payload)
                .unwrap();
            assert_eq!(batch.num_rows(), 1);
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            actual.push((region.region_number(), values.value(0)));
        }
        actual.sort_unstable();
        assert_eq!(actual, [(1, 70), (2, 20), (3, 1)]);

        // A leader-only change retains the original IPC buffers and schema hint.
        let request = build_bulk_request(3, versions[&3], vec![1]);
        let Some(Body::BulkInsert(bulk)) = request.body.as_ref() else {
            unreachable!()
        };
        let Some(bulk_insert_request::Body::ArrowIpc(ipc)) = &bulk.body else {
            unreachable!()
        };
        let pointers = (
            ipc.schema.as_ptr(),
            ipc.data_header.as_ptr(),
            ipc.payload.as_ptr(),
        );
        let rerouted = repartition_with_cached_table(&manager, &cache, request)
            .await
            .unwrap();
        assert_eq!(rerouted.len(), 1);
        let Some(Body::BulkInsert(bulk)) = rerouted[0].body.as_ref() else {
            unreachable!()
        };
        assert_eq!(
            bulk.aligned_schema_version,
            Some(AlignedSchemaVersion {
                schema_version: 123
            })
        );
        let Some(bulk_insert_request::Body::ArrowIpc(ipc)) = &bulk.body else {
            unreachable!()
        };
        assert_eq!(
            (
                ipc.schema.as_ptr(),
                ipc.data_header.as_ptr(),
                ipc.payload.as_ptr()
            ),
            pointers
        );
    }
    #[tokio::test]
    async fn test_repartition_logical_table_preserves_logical_region_ids() {
        let (manager, cache) = create_fixture().await;
        for insert in [true, false] {
            let mut request = build_row_request(build_rows(&[1, 20, 70]), insert, 99, Some(0));
            match request.body.as_mut().unwrap() {
                Body::Inserts(inserts) => {
                    inserts.requests[0].region_id = RegionId::new(42, 99).as_u64()
                }
                Body::Deletes(deletes) => {
                    deletes.requests[0].region_id = RegionId::new(42, 99).as_u64()
                }
                _ => unreachable!(),
            }
            let rerouted = repartition_with_cached_table(&manager, &cache, request)
                .await
                .unwrap();
            assert_eq!(rerouted.len(), 3);
            let mut regions = rerouted
                .iter()
                .map(|request| {
                    let region = RegionId::from_u64(row_request_parts(request).0);
                    assert_eq!(region.table_id(), 42);
                    region.region_number()
                })
                .collect::<Vec<_>>();
            regions.sort_unstable();
            assert_eq!(regions, [1, 2, 3]);
        }
    }

    async fn repartition_with_cached_table(
        manager: &PartitionRuleManager,
        cache: &TableInfoCache,
        request: RegionRequest,
    ) -> Result<Vec<RegionRequest>> {
        let region_id = match request.body.as_ref().unwrap() {
            Body::Inserts(inserts) => inserts.requests[0].region_id,
            Body::Deletes(deletes) => deletes.requests[0].region_id,
            Body::BulkInsert(bulk) => bulk.region_id,
            _ => unreachable!(),
        };
        let table_id = RegionId::from_u64(region_id).table_id();
        let table_info = cache.get(table_id).await.unwrap().unwrap();
        let partitioner = TablePartitioner::try_new(manager, &table_info)
            .await
            .context(error::InvalidPartitionSnafu)?;
        partitioner.repartition_request(request)
    }
}
