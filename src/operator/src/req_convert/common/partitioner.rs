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

use api::v1::region::{
    BulkInsertRequest, DeleteRequest, DeleteRequests, InsertRequest, InsertRequests, RegionRequest,
    bulk_insert_request, region_request::Body,
};
use api::v1::{ArrowIpc, PartitionExprVersion, Rows};
use common_error::ext::BoxedError;
use common_grpc::flight::{self, FlightDecoder};
use datatypes::arrow::array::{Array, BinaryArray, StringBuilder};
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::{DataType, Field, Schema};
use datatypes::arrow::record_batch::RecordBatch;
use mito_codec::row_converter::sparse::{SparseOffsetsCache, SparsePrimaryKeyView};
use partition::PartitionRuleRef;
use partition::manager::PartitionRuleManager;
use partition::partition::RegionMask;
use partition::splitter::RowSplitter;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::consts;
use store_api::storage::{RegionId, RegionNumber, TableId};
use table::metadata::TableInfo;

use crate::error::{self, Result, SplitDeleteSnafu, SplitInsertSnafu};
use crate::write_dispatcher::{WriteAttribution, write_row_count};

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
    pub(crate) fn rebuild_attributed_write(
        &self,
        request: RegionRequest,
        table_info: &TableInfo,
        attribution: WriteAttribution,
    ) -> Result<Vec<(RegionRequest, WriteAttribution)>> {
        if !attribution.is_tracked() {
            return Ok(self
                .rebuild_write_requests(request, table_info)?
                .into_iter()
                .map(|request| (request, WriteAttribution::default()))
                .collect());
        }
        let RegionRequest { header, body } = request;
        if let Some(Body::BulkInsert(bulk)) = body {
            return Ok(self
                .rebuild_bulk_insert_request(bulk, table_info, attribution)?
                .into_iter()
                .map(|(body, attribution)| {
                    (
                        RegionRequest {
                            header: header.clone(),
                            body: Some(body),
                        },
                        attribution,
                    )
                })
                .collect());
        }
        self.rebuild_write_requests(RegionRequest { header, body }, table_info)?
            .into_iter()
            .map(|request| {
                let attribution = attribution
                    .for_rows(write_row_count(&request))
                    .map_err(BoxedError::new)
                    .context(error::ExternalSnafu)?;
                Ok((request, attribution))
            })
            .collect()
    }

    pub fn partition_attributed_bulk_requests(
        &self,
        batch: RecordBatch,
        skip_wal: bool,
        attribution: WriteAttribution,
    ) -> Result<Vec<(BulkInsertRequest, WriteAttribution)>> {
        if !attribution.is_tracked() {
            return Ok(self
                .partition_bulk_insert_requests(batch, None, skip_wal)?
                .into_iter()
                .map(|request| (request, WriteAttribution::default()))
                .collect());
        }
        let masks = self
            .rule
            .split_record_batch(&batch)
            .context(SplitInsertSnafu)?;
        self.build_attributed_bulk_requests(batch, masks, None, skip_wal, attribution)
    }

    fn build_attributed_bulk_requests(
        &self,
        batch: RecordBatch,
        masks: HashMap<RegionNumber, RegionMask>,
        original_ipc: Option<ArrowIpc>,
        skip_wal: bool,
        attribution: WriteAttribution,
    ) -> Result<Vec<(BulkInsertRequest, WriteAttribution)>> {
        let mut by_region = masks
            .iter()
            .filter(|(_, mask)| !mask.select_none())
            .map(|(region, mask)| {
                Ok((
                    *region,
                    attribution
                        .filter(mask)
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        self.build_bulk_insert_requests(batch, masks, original_ipc, skip_wal)?
            .into_iter()
            .map(|request| {
                let attribution = by_region
                    .remove(&RegionId::from_u64(request.region_id).region_number())
                    .context(error::UnexpectedSnafu {
                        violated: "Missing write ownership after partitioning",
                    })?;
                Ok((request, attribution))
            })
            .collect()
    }

    /// Keeps process-local row ownership aligned when a failed write is repartitioned.
    pub(crate) fn rebuild_tracked_write_requests(
        &self,
        request: RegionRequest,
        table_info: &TableInfo,
        owners: Option<Vec<usize>>,
    ) -> Result<Vec<(RegionRequest, Option<Vec<usize>>)>> {
        let Some(owners) = owners else {
            return Ok(self
                .rebuild_write_requests(request, table_info)?
                .into_iter()
                .map(|request| (request, None))
                .collect());
        };
        let RegionRequest { header, body } = request;
        if let Some(Body::BulkInsert(bulk)) = body {
            let region_id = RegionId::from_u64(bulk.region_id);
            if self.is_partition_expr_unchanged(region_id, bulk.partition_expr_version) {
                return Ok(vec![(
                    RegionRequest {
                        header,
                        body: Some(Body::BulkInsert(bulk)),
                    },
                    Some(owners),
                )]);
            }
            let bulk_insert_request::Body::ArrowIpc(ipc) =
                bulk.body.context(error::UnexpectedSnafu {
                    violated: "Cannot rebuild a bulk insert without Arrow IPC data",
                })?;
            let batch = FlightDecoder::try_from_schema_bytes(&ipc.schema)
                .and_then(|mut decoder| {
                    decoder.try_decode_record_batch(&ipc.data_header, &ipc.payload)
                })
                .map_err(BoxedError::new)
                .context(error::ExternalSnafu)?;
            let routing = self.restore_partition_tags(&batch, table_info)?;
            let masks = self
                .rule
                .split_record_batch(routing.as_ref().unwrap_or(&batch))
                .context(SplitInsertSnafu)?;
            return self
                .build_tracked_bulk_insert_requests(batch, masks, owners, bulk.skip_wal)
                .map(|requests| {
                    requests
                        .into_iter()
                        .map(|(request, owners)| {
                            (
                                RegionRequest {
                                    header: header.clone(),
                                    body: Some(Body::BulkInsert(request)),
                                },
                                Some(owners),
                            )
                        })
                        .collect()
                });
        }
        let owner = owners.first().copied().context(error::UnexpectedSnafu {
            violated: "Missing row ownership",
        })?;
        ensure!(
            owners.iter().all(|value| *value == owner),
            error::UnexpectedSnafu {
                violated: "Mixed row ownership requires Arrow batching"
            }
        );
        self.rebuild_write_requests(RegionRequest { header, body }, table_info)?
            .into_iter()
            .map(|request| {
                let count = match &request.body {
                    Some(Body::Inserts(inserts)) => inserts
                        .requests
                        .iter()
                        .filter_map(|r| r.rows.as_ref())
                        .map(|r| r.rows.len())
                        .sum(),
                    _ => {
                        return error::UnexpectedSnafu {
                            violated: "Unsupported tracked write",
                        }
                        .fail();
                    }
                };
                Ok((request, Some(vec![owner; count])))
            })
            .collect()
    }

    pub(crate) fn partition_tracked_bulk_insert_requests(
        &self,
        batch: RecordBatch,
        owners: Vec<usize>,
        skip_wal: bool,
    ) -> Result<Vec<(BulkInsertRequest, Vec<usize>)>> {
        let masks = self
            .rule
            .split_record_batch(&batch)
            .context(SplitInsertSnafu)?;
        self.build_tracked_bulk_insert_requests(batch, masks, owners, skip_wal)
    }

    fn build_tracked_bulk_insert_requests(
        &self,
        batch: RecordBatch,
        masks: HashMap<RegionNumber, RegionMask>,
        owners: Vec<usize>,
        skip_wal: bool,
    ) -> Result<Vec<(BulkInsertRequest, Vec<usize>)>> {
        ensure!(
            owners.len() == batch.num_rows(),
            error::UnexpectedSnafu {
                violated: "Write ownership does not match row count"
            }
        );
        let mut selected = HashMap::new();
        for (region, mask) in &masks {
            let mut selected_owners = Vec::with_capacity(mask.selected_rows());
            selected_owners.extend(
                mask.array()
                    .values()
                    .set_indices()
                    .map(|index| owners[index]),
            );
            selected.insert(
                RegionId::new(self.table_id, *region).as_u64(),
                selected_owners,
            );
        }
        self.build_bulk_insert_requests(batch, masks, None, skip_wal)?
            .into_iter()
            .map(|request| {
                let owners =
                    selected
                        .remove(&request.region_id)
                        .context(error::UnexpectedSnafu {
                            violated: "Missing partition ownership",
                        })?;
                Ok((request, owners))
            })
            .collect()
    }

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

    /// Rebuilds write requests using this partition snapshot after a route refresh.
    ///
    /// Takes ownership so row payloads can move into their new regions. Requests whose
    /// region and partition version still match retain their original payloads.
    /// The table metadata comes from the same refresh that created this partitioner.
    pub(crate) fn rebuild_write_requests(
        &self,
        request: RegionRequest,
        table_info: &TableInfo,
    ) -> Result<Vec<RegionRequest>> {
        let RegionRequest { header, body } = request;
        let bodies = match body.context(error::UnexpectedSnafu {
            violated: "Cannot rebuild a request without a body",
        })? {
            Body::Inserts(inserts) => self.rebuild_insert_requests(inserts)?,
            Body::Deletes(deletes) => self.rebuild_delete_requests(deletes)?,
            Body::BulkInsert(bulk) => self
                .rebuild_bulk_insert_request(bulk, table_info, WriteAttribution::default())?
                .into_iter()
                .map(|(body, _)| body)
                .collect(),
            _ => {
                return error::UnexpectedSnafu {
                    violated: "Only writes can be rebuilt",
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
        original_ipc: Option<ArrowIpc>,
        skip_wal: bool,
    ) -> Result<Vec<BulkInsertRequest>> {
        let masks = self
            .rule
            .split_record_batch(&batch)
            .context(SplitInsertSnafu)?;
        self.build_bulk_insert_requests(batch, masks, original_ipc, skip_wal)
    }

    fn build_bulk_insert_requests(
        &self,
        batch: RecordBatch,
        masks: HashMap<RegionNumber, RegionMask>,
        mut original_ipc: Option<ArrowIpc>,
        skip_wal: bool,
    ) -> Result<Vec<BulkInsertRequest>> {
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

    fn rebuild_insert_requests(&self, inserts: InsertRequests) -> Result<Vec<Body>> {
        let mut bodies = Vec::new();
        for insert in inserts.requests {
            let region_id = RegionId::from_u64(insert.region_id);
            if self.is_partition_expr_unchanged(region_id, insert.partition_expr_version) {
                bodies.push(Body::Inserts(InsertRequests {
                    requests: vec![insert],
                }));
                continue;
            }
            let rows = insert.rows.context(error::UnexpectedSnafu {
                violated: "Cannot rebuild an insert without rows",
            })?;
            for request in self.partition_insert_requests(rows, insert.skip_wal)? {
                bodies.push(Body::Inserts(InsertRequests {
                    requests: vec![request],
                }));
            }
        }
        Ok(bodies)
    }

    fn rebuild_delete_requests(&self, deletes: DeleteRequests) -> Result<Vec<Body>> {
        let mut bodies = Vec::new();
        for delete in deletes.requests {
            let region_id = RegionId::from_u64(delete.region_id);
            if self.is_partition_expr_unchanged(region_id, delete.partition_expr_version) {
                bodies.push(Body::Deletes(DeleteRequests {
                    requests: vec![delete],
                }));
                continue;
            }
            let rows = delete.rows.context(error::UnexpectedSnafu {
                violated: "Cannot rebuild a delete without rows",
            })?;
            for request in self.partition_delete_requests(rows)? {
                bodies.push(Body::Deletes(DeleteRequests {
                    requests: vec![request],
                }));
            }
        }
        Ok(bodies)
    }

    fn rebuild_bulk_insert_request(
        &self,
        bulk: BulkInsertRequest,
        table_info: &TableInfo,
        attribution: WriteAttribution,
    ) -> Result<Vec<(Body, WriteAttribution)>> {
        let region_id = RegionId::from_u64(bulk.region_id);
        if self.is_partition_expr_unchanged(region_id, bulk.partition_expr_version) {
            return Ok(vec![(Body::BulkInsert(bulk), attribution)]);
        }
        let bulk_insert_request::Body::ArrowIpc(ipc) =
            bulk.body.context(error::UnexpectedSnafu {
                violated: "Cannot rebuild a bulk insert without Arrow IPC data",
            })?;
        let batch = FlightDecoder::try_from_schema_bytes(&ipc.schema)
            .and_then(|mut decoder| decoder.try_decode_record_batch(&ipc.data_header, &ipc.payload))
            .map_err(BoxedError::new)
            .context(error::ExternalSnafu)?;
        let routing_batch = self.restore_partition_tags(&batch, table_info)?;
        let masks = self
            .rule
            .split_record_batch(routing_batch.as_ref().unwrap_or(&batch))
            .context(SplitInsertSnafu)?;
        let writes = if attribution.is_tracked() {
            self.build_attributed_bulk_requests(
                batch,
                masks,
                Some(ipc),
                bulk.skip_wal,
                attribution,
            )?
        } else {
            self.build_bulk_insert_requests(batch, masks, Some(ipc), bulk.skip_wal)?
                .into_iter()
                .map(|request| (request, WriteAttribution::default()))
                .collect()
        };
        Ok(writes
            .into_iter()
            .map(|(request, attribution)| (Body::BulkInsert(request), attribution))
            .collect())
    }

    /// Restores omitted physical-table tags for routing without changing the write payload.
    fn restore_partition_tags(
        &self,
        batch: &RecordBatch,
        table_info: &TableInfo,
    ) -> Result<Option<RecordBatch>> {
        let meta = &table_info.meta;
        if !table_info.is_physical_table() || meta.column_ids.len() != meta.schema.num_columns() {
            return Ok(None);
        }
        let missing = self
            .rule
            .partition_columns()
            .iter()
            .filter(|name| batch.column_by_name(name).is_none())
            .filter_map(|name| {
                let index = meta.schema.column_index_by_name(name)?;
                (meta.primary_key_indices.contains(&index)
                    && meta.schema.column_schemas()[index].data_type.is_string())
                .then_some((name, meta.column_ids[index]))
            })
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(None);
        }
        let Some(primary_key) = batch.column_by_name(consts::PRIMARY_KEY_COLUMN_NAME) else {
            return Ok(None);
        };
        let primary_key =
            primary_key
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context(error::UnexpectedSnafu {
                    violated: "Sparse primary key column must be Binary",
                })?;
        let mut builders = missing
            .iter()
            .map(|_| StringBuilder::new())
            .collect::<Vec<_>>();
        let mut cache = SparseOffsetsCache::new();
        let mut buffer = Vec::new();
        for row in 0..batch.num_rows() {
            ensure!(
                !primary_key.is_null(row),
                error::UnexpectedSnafu {
                    violated: "Sparse primary key must not be null",
                }
            );
            let mut key = SparsePrimaryKeyView::new(primary_key.value(row), &mut cache)
                .map_err(BoxedError::new)
                .context(error::ExternalSnafu)?;
            for ((_, column_id), builder) in missing.iter().zip(&mut builders) {
                let label = key
                    .label(*column_id, &mut buffer)
                    .map_err(BoxedError::new)
                    .context(error::ExternalSnafu)?;
                builder.append_option(label);
            }
        }
        let schema = batch.schema();
        let mut fields = schema.fields().to_vec();
        let mut columns = batch.columns().to_vec();
        for ((name, _), mut builder) in missing.into_iter().zip(builders) {
            fields.push(Arc::new(Field::new(name, DataType::Utf8, true)));
            columns.push(Arc::new(builder.finish()));
        }
        RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
            columns,
        )
        .map(Some)
        .context(error::ComputeArrowSnafu)
    }

    /// Whether the request's region still exists with the same partition expression version.
    ///
    /// A leader change leaves the partition unchanged, so the original payload can be
    /// sent to the new peer. A removed region or changed partition expression version
    /// requires rebuilding the request using this snapshot's partition rule.
    fn is_partition_expr_unchanged(
        &self,
        region_id: RegionId,
        version: Option<PartitionExprVersion>,
    ) -> bool {
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
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_meta::rpc::store::RangeResponse;
    use datatypes::arrow::array::{BooleanArray, Int32Array, StringArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema;
    use datatypes::value::{Value as DataValue, ValueRef};
    use mito_codec::row_converter::sparse;
    use mito_codec::row_converter::{PrimaryKeyCodec, SparsePrimaryKeyCodec};
    use moka::future::Cache;
    use partition::expr;
    use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};
    use partition::multi_dim::MultiDimPartitionRule;
    use store_api::metric_engine_consts;

    use super::*;
    use crate::error::Error;
    use crate::test_util;
    use crate::test_util::{test_new_partition_info_cache, test_new_table_route_cache};

    fn build_sparse_partitioner() -> TablePartitioner {
        TablePartitioner {
            table_id: 1,
            rule: Arc::new(
                MultiDimPartitionRule::try_new(
                    vec!["zone".to_string()],
                    vec![1, 2],
                    vec![
                        expr::col("zone").lt(DataValue::String("m".into())),
                        expr::col("zone").gt_eq(DataValue::String("m".into())),
                    ],
                    true,
                )
                .unwrap(),
            ),
            versions: [
                (1, Some(PartitionExprVersion { value: 2 })),
                (2, Some(PartitionExprVersion { value: 2 })),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn build_sparse_table_info() -> TableInfo {
        let mut table_info = test_util::new_test_table_info(1, "physical_tags", [1, 2].into_iter());
        table_info.meta.schema = Arc::new(schema::Schema::new(vec![
            schema::ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            schema::ColumnSchema::new("zone", ConcreteDataType::string_datatype(), true),
        ]));
        table_info.meta.primary_key_indices = vec![0, 1];
        table_info.meta.partition_key_indices = vec![1];
        table_info.meta.column_ids = vec![10, 20];
        table_info.meta.options.extra_options.insert(
            metric_engine_consts::PHYSICAL_TABLE_METADATA_KEY.to_string(),
            String::new(),
        );
        table_info
    }

    fn build_sparse_batch() -> RecordBatch {
        let codec = SparsePrimaryKeyCodec::schemaless();
        let keys = [Some("east"), Some("west"), Some(""), None, None]
            .into_iter()
            .enumerate()
            .map(|(index, zone)| {
                let mut key = Vec::new();
                let values = [
                    (
                        sparse::RESERVED_COLUMN_ID_TABLE_ID,
                        ValueRef::UInt32(42 + index as u32),
                    ),
                    (
                        sparse::RESERVED_COLUMN_ID_TSID,
                        ValueRef::UInt64(index as u64),
                    ),
                    (10, ValueRef::String("node")),
                ];
                codec.encode_value_refs(&values, &mut key).unwrap();
                if let Some(zone) = zone {
                    codec
                        .encode_raw_tag_value([(20, zone.as_bytes())].into_iter(), &mut key)
                        .unwrap();
                }
                if index == 4 {
                    key.extend_from_slice(&20_u32.to_be_bytes());
                    key.push(0);
                }
                key
            })
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("host", DataType::Utf8, false),
                Field::new(consts::PRIMARY_KEY_COLUMN_NAME, DataType::Binary, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["node"; keys.len()])),
                Arc::new(BinaryArray::from_iter_values(keys.iter())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_partition_tags_use_physical_table_metadata() {
        let backend = Arc::new(MemoryKvBackend::new());
        let table_route_cache = test_new_table_route_cache(backend.clone());
        let partition_info_cache = test_new_partition_info_cache(table_route_cache.clone());
        let manager =
            PartitionRuleManager::new(backend.clone(), table_route_cache, partition_info_cache);
        let table_info = build_sparse_table_info();
        let routes = [
            expr::col("zone").lt(DataValue::String("m".into())),
            expr::col("zone").gt_eq(DataValue::String("m".into())),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, expr)| RegionRoute {
            region: Region {
                id: RegionId::new(1, index as u32 + 1),
                partition_expr: expr.as_json_str().unwrap(),
                ..Default::default()
            },
            ..Default::default()
        })
        .collect();
        TableMetadataManager::new(backend)
            .create_table_metadata(
                table_info.clone(),
                TableRouteValue::physical(routes),
                Default::default(),
            )
            .await
            .unwrap();
        let batch = build_sparse_batch();
        let partitioner = TablePartitioner::try_new(&manager, &table_info)
            .await
            .unwrap();
        let restored = partitioner
            .restore_partition_tags(&batch, &table_info)
            .unwrap()
            .unwrap();
        let zones = restored
            .column_by_name("zone")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            zones.iter().collect::<Vec<_>>(),
            vec![Some("east"), Some("west"), Some(""), None, None]
        );

        for (physical, tag, valid_ids) in [
            (false, true, true),
            (true, false, true),
            (true, true, false),
        ] {
            let mut info = table_info.clone();
            if !physical {
                info.meta
                    .options
                    .extra_options
                    .remove(metric_engine_consts::PHYSICAL_TABLE_METADATA_KEY);
            }
            if !tag {
                info.meta.primary_key_indices = vec![0];
            }
            if !valid_ids {
                info.meta.column_ids.clear();
            }
            let partitioner = TablePartitioner::try_new(&manager, &info).await.unwrap();
            assert!(
                partitioner
                    .restore_partition_tags(&batch, &info)
                    .unwrap()
                    .is_none()
            );
            assert!(
                partitioner
                    .partition_bulk_insert_requests(batch.clone(), None, false)
                    .is_err()
            );
        }
    }

    #[test]
    fn test_rebuild_sparse_bulk_with_changed_partition_column() {
        let partitioner = build_sparse_partitioner();
        let table_info = build_sparse_table_info();
        let batch = build_sparse_batch();
        let restored = partitioner
            .restore_partition_tags(&batch, &table_info)
            .unwrap()
            .unwrap();
        let zones = restored
            .column_by_name("zone")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            zones.iter().collect::<Vec<_>>(),
            vec![Some("east"), Some("west"), Some(""), None, None]
        );
        let (schema, data_header, payload) = flight::record_batch_to_ipc(batch.clone()).unwrap();
        let rebuilt = partitioner
            .rebuild_write_requests(
                build_request(Body::BulkInsert(BulkInsertRequest {
                    region_id: RegionId::new(1, 1).as_u64(),
                    partition_expr_version: Some(PartitionExprVersion { value: 1 }),
                    skip_wal: true,
                    body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                        schema,
                        data_header,
                        payload,
                    })),
                    ..Default::default()
                })),
                &table_info,
            )
            .unwrap();
        assert_eq!(rebuilt.len(), 2);
        for request in rebuilt {
            let Some(Body::BulkInsert(bulk)) = request.body else {
                unreachable!()
            };
            assert!(bulk.skip_wal);
            assert_eq!(
                bulk.partition_expr_version,
                Some(PartitionExprVersion { value: 2 })
            );
            let Some(bulk_insert_request::Body::ArrowIpc(ipc)) = bulk.body else {
                unreachable!()
            };
            let actual = FlightDecoder::try_from_schema_bytes(&ipc.schema)
                .unwrap()
                .try_decode_record_batch(&ipc.data_header, &ipc.payload)
                .unwrap();
            let mask = if RegionId::from_u64(bulk.region_id).region_number() == 1 {
                vec![true, false, true, true, true]
            } else {
                vec![false, true, false, false, false]
            };
            let expected = compute::filter_record_batch(&batch, &mask.into()).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_restore_partition_tags_checks_only_missing_tags() {
        let partitioner = build_sparse_partitioner();
        let table_info = build_sparse_table_info();
        let batch = build_sparse_batch();
        let invalid = RecordBatch::try_new(
            batch.schema(),
            vec![
                batch.column(0).clone(),
                Arc::new(BinaryArray::from_iter_values([b"invalid"; 5])),
            ],
        )
        .unwrap();
        assert!(
            partitioner
                .restore_partition_tags(&invalid, &table_info)
                .is_err()
        );
        let mut fields = invalid.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("zone", DataType::Utf8, true)));
        let mut columns = invalid.columns().to_vec();
        columns.push(Arc::new(StringArray::from(vec![Some("east"); 5])));
        let complete = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        assert!(
            partitioner
                .restore_partition_tags(&complete, &table_info)
                .unwrap()
                .is_none()
        );
        let (schema, data_header, payload) = flight::record_batch_to_ipc(invalid).unwrap();
        let request = build_request(Body::BulkInsert(BulkInsertRequest {
            region_id: RegionId::new(1, 1).as_u64(),
            partition_expr_version: Some(PartitionExprVersion { value: 2 }),
            body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                schema,
                data_header,
                payload,
            })),
            ..Default::default()
        }));
        let expected = request.clone();
        assert_eq!(
            partitioner
                .rebuild_write_requests(request, &table_info)
                .unwrap(),
            vec![expected]
        );
    }

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
            Cache::new(16),
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
    async fn test_rebuild_rows_updates_partitions_without_losing_rows() {
        let (manager, cache) = create_fixture().await;
        let table_info = cache.get(1).await.unwrap().unwrap();
        let (_, versions) = manager
            .find_table_partition_rule(&table_info)
            .await
            .unwrap();
        for insert in [true, false] {
            let request = build_row_request(build_rows(&[1, 20, 70]), insert, 99, Some(0));
            let header = request.header.clone();
            let rerouted = rebuild_with_cached_table(&manager, &cache, request)
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
            let rerouted = rebuild_with_cached_table(&manager, &cache, request)
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
    async fn test_rebuild_bulk_reencodes_only_changed_partitions() {
        let (manager, cache) = create_fixture().await;
        let table_info = cache.get(1).await.unwrap().unwrap();
        let (_, versions) = manager
            .find_table_partition_rule(&table_info)
            .await
            .unwrap();
        let request = build_bulk_request(99, Some(0), vec![1, 20, 70]);
        let header = request.header.clone();
        let rerouted = rebuild_with_cached_table(&manager, &cache, request)
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
            let batch = FlightDecoder::try_from_schema_bytes(&ipc.schema)
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
        let rerouted = rebuild_with_cached_table(&manager, &cache, request)
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
    async fn test_rebuild_logical_table_preserves_logical_region_ids() {
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
            let rerouted = rebuild_with_cached_table(&manager, &cache, request)
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

    async fn rebuild_with_cached_table(
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
        partitioner.rebuild_write_requests(request, &table_info)
    }

    #[tokio::test]
    async fn test_tracked_bulk_owners_preserve_payload_alignment() {
        let (manager, cache) = create_fixture().await;
        let table_info = cache.get(1).await.unwrap().unwrap();
        let partitioner = TablePartitioner::try_new(&manager, &table_info)
            .await
            .unwrap();
        for len in [0, 1, 7, 64, 65, 257] {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from_iter_values(0..len as i32))],
            )
            .unwrap();
            let owners = (0..len)
                .map(|i| [2, 2, 0, 1, 0, 0][i % 6])
                .collect::<Vec<_>>();
            for offset in [0, 1, 7, 65] {
                for pattern in 0..3 {
                    let values = (0..offset + len)
                        .map(|i| match pattern {
                            0 => false,
                            1 => true,
                            _ => i % 3 != 0,
                        })
                        .collect::<Vec<_>>();
                    let expected = (0..len).filter(|i| values[offset + i]).collect::<Vec<_>>();
                    let mask = BooleanArray::from(values).slice(offset, len);
                    let requests = partitioner
                        .build_tracked_bulk_insert_requests(
                            batch.clone(),
                            [(1, RegionMask::from(mask))].into_iter().collect(),
                            owners.clone(),
                            false,
                        )
                        .unwrap();
                    if expected.is_empty() {
                        assert!(requests.is_empty());
                        continue;
                    }
                    assert_eq!(requests.len(), 1);
                    let (request, selected_owners) = &requests[0];
                    let Some(bulk_insert_request::Body::ArrowIpc(ipc)) = &request.body else {
                        panic!("expected Arrow IPC")
                    };
                    let payload = FlightDecoder::try_from_schema_bytes(&ipc.schema)
                        .unwrap()
                        .try_decode_record_batch(&ipc.data_header, &ipc.payload)
                        .unwrap();
                    let actual_rows = payload
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    assert_eq!(
                        actual_rows
                            .values()
                            .iter()
                            .map(|x| *x as usize)
                            .collect::<Vec<_>>(),
                        expected
                    );
                    assert_eq!(
                        *selected_owners,
                        expected.iter().map(|i| owners[*i]).collect::<Vec<_>>()
                    );
                }
            }
            let wrong = partitioner.build_tracked_bulk_insert_requests(
                batch,
                HashMap::new(),
                vec![0; len + 1],
                false,
            );
            assert!(wrong.is_err());
        }
    }
}
