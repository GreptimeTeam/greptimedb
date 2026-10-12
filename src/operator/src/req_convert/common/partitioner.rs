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

use api::v1::region::{BulkInsertRequest, DeleteRequest, InsertRequest, bulk_insert_request};
use api::v1::{ArrowIpc, PartitionExprVersion, Rows};
use common_error::ext::BoxedError;
use common_grpc::flight;
use datatypes::arrow::compute;
use datatypes::arrow::record_batch::RecordBatch;
use partition::PartitionRuleRef;
use partition::manager::PartitionRuleManager;
use partition::splitter::RowSplitter;
use snafu::ResultExt;
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
    versions: HashMap<RegionNumber, Option<u64>>,
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
            .await?
            .partition_insert_requests(rows, skip_wal)
    }

    /// Routes delete rows using the current table partitions.
    pub async fn partition_delete_requests(
        &self,
        table_info: &TableInfo,
        rows: Rows,
    ) -> Result<Vec<DeleteRequest>> {
        TablePartitioner::try_new(self.partition_manager, table_info)
            .await?
            .partition_delete_requests(rows)
    }
}

impl TablePartitioner {
    /// Reads a table's partition rule and expression versions into one routing snapshot.
    pub async fn try_new(
        partition_manager: &PartitionRuleManager,
        table_info: &TableInfo,
    ) -> Result<Self> {
        let (rule, versions) = partition_manager
            .find_table_partition_rule(table_info)
            .await
            .context(error::InvalidPartitionSnafu)?;
        Ok(Self {
            table_id: table_info.table_id(),
            rule,
            versions,
        })
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

    fn version(&self, region: RegionNumber) -> Option<PartitionExprVersion> {
        self.versions
            .get(&region)
            .copied()
            .flatten()
            .map(|value| PartitionExprVersion { value })
    }
}
