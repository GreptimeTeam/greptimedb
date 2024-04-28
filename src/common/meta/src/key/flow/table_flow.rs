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

use futures::stream::BoxStream;
use lazy_static::lazy_static;
use regex::Regex;
use snafu::OptionExt;
use table::metadata::TableId;

use crate::error::{self, Result};
use crate::key::flow::FlowTaskScoped;
use crate::key::scope::{BytesAdapter, CatalogScoped, MetaKey};
use crate::key::{FlowTaskId, FlowTaskPartitionId};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;
use crate::FlownodeId;

const TABLE_FLOW_KEY_PREFIX: &str = "source_table";

lazy_static! {
    static ref TABLE_FLOW_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{TABLE_FLOW_KEY_PREFIX}/([0-9]+)/([0-9]+)/([0-9]+)/([0-9]+)$"
    ))
    .unwrap();
}

/// The key of mapping [TableId] to [FlownodeId] and [FlowTaskId].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TableFlowKeyInner {
    table_id: TableId,
    flownode_id: FlownodeId,
    flow_id: FlowTaskId,
    partition_id: FlowTaskPartitionId,
}

/// The key of mapping [TableId] to [FlownodeId] and [FlowTaskId].
///
/// The layout: `__flow/{catalog}/table/{table_id}/{flownode_id}/{flow_id}/{partition_id}`.
#[derive(Debug, PartialEq)]
pub struct TableFlowKey(FlowTaskScoped<CatalogScoped<TableFlowKeyInner>>);

impl MetaKey<TableFlowKey> for TableFlowKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<TableFlowKey> {
        Ok(TableFlowKey(FlowTaskScoped::<
            CatalogScoped<TableFlowKeyInner>,
        >::from_bytes(bytes)?))
    }
}

impl TableFlowKey {
    /// Returns a new [TableFlowKey].
    pub fn new(
        catalog: String,
        table_id: TableId,
        flownode_id: FlownodeId,
        flow_id: FlowTaskId,
        partition_id: FlowTaskPartitionId,
    ) -> TableFlowKey {
        let inner = TableFlowKeyInner::new(table_id, flownode_id, flow_id, partition_id);
        TableFlowKey(FlowTaskScoped::new(CatalogScoped::new(catalog, inner)))
    }

    /// The prefix used to retrieve all [TableFlowKey]s with the specified `table_id`.
    pub fn range_start_key(catalog: String, table_id: TableId) -> Vec<u8> {
        let catalog_scoped_key = CatalogScoped::new(
            catalog,
            BytesAdapter::from(TableFlowKeyInner::range_start_key(table_id).into_bytes()),
        );

        FlowTaskScoped::new(catalog_scoped_key).to_bytes()
    }

    /// Returns the catalog.
    pub fn catalog(&self) -> &str {
        self.0.catalog()
    }

    /// Returns the source [TableId].
    pub fn source_table_id(&self) -> TableId {
        self.0.table_id
    }

    /// Returns the [FlowTaskId].
    pub fn flow_id(&self) -> FlowTaskId {
        self.0.flow_id
    }

    /// Returns the [FlownodeId].
    pub fn flownode_id(&self) -> FlownodeId {
        self.0.flownode_id
    }

    /// Returns the [PartitionId].
    pub fn partition_id(&self) -> FlowTaskPartitionId {
        self.0.partition_id
    }
}

impl TableFlowKeyInner {
    /// Returns a new [TableFlowKey].
    fn new(
        table_id: TableId,
        flownode_id: FlownodeId,
        flow_id: FlowTaskId,
        partition_id: FlowTaskPartitionId,
    ) -> TableFlowKeyInner {
        Self {
            table_id,
            flownode_id,
            flow_id,
            partition_id,
        }
    }

    fn prefix(table_id: TableId) -> String {
        format!("{}/{table_id}", TABLE_FLOW_KEY_PREFIX)
    }

    /// The prefix used to retrieve all [TableFlowKey]s with the specified `table_id`.
    fn range_start_key(table_id: TableId) -> String {
        format!("{}/", Self::prefix(table_id))
    }
}

impl MetaKey<TableFlowKeyInner> for TableFlowKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!(
            "{TABLE_FLOW_KEY_PREFIX}/{}/{}/{}/{}",
            self.table_id, self.flownode_id, self.flow_id, self.partition_id
        )
        .into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<TableFlowKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidTableMetadataSnafu {
                err_msg: format!(
                    "TableFlowKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures =
            TABLE_FLOW_KEY_PATTERN
                .captures(key)
                .context(error::InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid TableFlowKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let table_id = captures[1].parse::<TableId>().unwrap();
        let flownode_id = captures[2].parse::<FlownodeId>().unwrap();
        let flow_id = captures[3].parse::<FlowTaskId>().unwrap();
        let partition_id = captures[4].parse::<FlowTaskPartitionId>().unwrap();
        Ok(TableFlowKeyInner::new(
            table_id,
            flownode_id,
            flow_id,
            partition_id,
        ))
    }
}

/// Decodes `KeyValue` to [TableFlowKey].
pub fn table_task_decoder(kv: KeyValue) -> Result<TableFlowKey> {
    TableFlowKey::from_bytes(&kv.key)
}

/// The manager of [TableFlowKey].
pub struct TableTaskManager {
    kv_backend: KvBackendRef,
}

impl TableTaskManager {
    /// Returns a new [TableTaskManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Retrieves all [TableFlowKey]s of the specified `table_id`.
    pub fn nodes(
        &self,
        catalog: &str,
        table_id: TableId,
    ) -> BoxStream<'static, Result<TableFlowKey>> {
        let start_key = TableFlowKey::range_start_key(catalog.to_string(), table_id);
        let req = RangeRequest::new().with_prefix(start_key);
        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(table_task_decoder),
        );

        Box::pin(stream)
    }

    /// Builds a create table task transaction.
    ///
    /// Puts `__table_task/{table_id}/{node_id}/{partition_id}` keys.
    pub fn build_create_txn<I: IntoIterator<Item = (FlowTaskPartitionId, FlownodeId)>>(
        &self,
        catalog: &str,
        flow_id: FlowTaskId,
        flownode_ids: I,
        source_table_ids: &[TableId],
    ) -> Txn {
        let txns = flownode_ids
            .into_iter()
            .flat_map(|(partition_id, flownode_id)| {
                source_table_ids.iter().map(move |table_id| {
                    TxnOp::Put(
                        TableFlowKey::new(
                            catalog.to_string(),
                            *table_id,
                            flownode_id,
                            flow_id,
                            partition_id,
                        )
                        .to_bytes(),
                        vec![],
                    )
                })
            })
            .collect::<Vec<_>>();

        Txn::new().and_then(txns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_serialization() {
        let table_task_key = TableFlowKey::new("my_catalog".to_string(), 1024, 1, 2, 0);
        assert_eq!(
            b"__flow/my_catalog/source_table/1024/1/2/0".to_vec(),
            table_task_key.to_bytes(),
        );
        let prefix = TableFlowKey::range_start_key("my_catalog".to_string(), 1024);
        assert_eq!(b"__flow/my_catalog/source_table/1024/".to_vec(), prefix);
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow/my_catalog/source_table/1024/1/2/0".to_vec();
        let key = TableFlowKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.catalog(), "my_catalog");
        assert_eq!(key.source_table_id(), 1024);
        assert_eq!(key.flownode_id(), 1);
        assert_eq!(key.flow_id(), 2);
        assert_eq!(key.partition_id(), 0);
    }
}
