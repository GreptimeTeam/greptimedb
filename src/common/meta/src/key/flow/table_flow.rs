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
use crate::key::flow::FlowScoped;
use crate::key::{BytesAdapter, FlowId, FlowPartitionId, MetaKey};
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

/// The key of mapping [TableId] to [FlownodeId] and [FlowId].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TableFlowKeyInner {
    table_id: TableId,
    flownode_id: FlownodeId,
    flow_id: FlowId,
    partition_id: FlowPartitionId,
}

/// The key of mapping [TableId] to [FlownodeId] and [FlowId].
///
/// The layout: `__flow/table/{table_id}/{flownode_id}/{flow_id}/{partition_id}`.
#[derive(Debug, PartialEq)]
pub struct TableFlowKey(FlowScoped<TableFlowKeyInner>);

impl MetaKey<TableFlowKey> for TableFlowKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<TableFlowKey> {
        Ok(TableFlowKey(FlowScoped::<TableFlowKeyInner>::from_bytes(
            bytes,
        )?))
    }
}

impl TableFlowKey {
    /// Returns a new [TableFlowKey].
    pub fn new(
        table_id: TableId,
        flownode_id: FlownodeId,
        flow_id: FlowId,
        partition_id: FlowPartitionId,
    ) -> TableFlowKey {
        let inner = TableFlowKeyInner::new(table_id, flownode_id, flow_id, partition_id);
        TableFlowKey(FlowScoped::new(inner))
    }

    /// The prefix used to retrieve all [TableFlowKey]s with the specified `table_id`.
    pub fn range_start_key(table_id: TableId) -> Vec<u8> {
        let inner = BytesAdapter::from(TableFlowKeyInner::range_start_key(table_id).into_bytes());

        FlowScoped::new(inner).to_bytes()
    }

    /// Returns the source [TableId].
    pub fn source_table_id(&self) -> TableId {
        self.0.table_id
    }

    /// Returns the [FlowId].
    pub fn flow_id(&self) -> FlowId {
        self.0.flow_id
    }

    /// Returns the [FlownodeId].
    pub fn flownode_id(&self) -> FlownodeId {
        self.0.flownode_id
    }

    /// Returns the [PartitionId].
    pub fn partition_id(&self) -> FlowPartitionId {
        self.0.partition_id
    }
}

impl TableFlowKeyInner {
    /// Returns a new [TableFlowKey].
    fn new(
        table_id: TableId,
        flownode_id: FlownodeId,
        flow_id: FlowId,
        partition_id: FlowPartitionId,
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
        let flow_id = captures[3].parse::<FlowId>().unwrap();
        let partition_id = captures[4].parse::<FlowPartitionId>().unwrap();
        Ok(TableFlowKeyInner::new(
            table_id,
            flownode_id,
            flow_id,
            partition_id,
        ))
    }
}

/// Decodes `KeyValue` to [TableFlowKey].
pub fn table_flow_decoder(kv: KeyValue) -> Result<TableFlowKey> {
    TableFlowKey::from_bytes(&kv.key)
}

/// The manager of [TableFlowKey].
pub struct TableFlowManager {
    kv_backend: KvBackendRef,
}

impl TableFlowManager {
    /// Returns a new [TableFlowManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Retrieves all [TableFlowKey]s of the specified `table_id`.
    pub fn nodes(&self, table_id: TableId) -> BoxStream<'static, Result<TableFlowKey>> {
        let start_key = TableFlowKey::range_start_key(table_id);
        let req = RangeRequest::new().with_prefix(start_key);
        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(table_flow_decoder),
        );

        Box::pin(stream)
    }

    /// Builds a create table flow transaction.
    ///
    /// Puts `__table_flow/{table_id}/{node_id}/{partition_id}` keys.
    pub fn build_create_txn<I: IntoIterator<Item = (FlowPartitionId, FlownodeId)>>(
        &self,
        flow_id: FlowId,
        flownode_ids: I,
        source_table_ids: &[TableId],
    ) -> Txn {
        let txns = flownode_ids
            .into_iter()
            .flat_map(|(partition_id, flownode_id)| {
                source_table_ids.iter().map(move |table_id| {
                    TxnOp::Put(
                        TableFlowKey::new(*table_id, flownode_id, flow_id, partition_id).to_bytes(),
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
        let table_flow_key = TableFlowKey::new(1024, 1, 2, 0);
        assert_eq!(
            b"__flow/source_table/1024/1/2/0".to_vec(),
            table_flow_key.to_bytes(),
        );
        let prefix = TableFlowKey::range_start_key(1024);
        assert_eq!(b"__flow/source_table/1024/".to_vec(), prefix);
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow/source_table/1024/1/2/0".to_vec();
        let key = TableFlowKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.source_table_id(), 1024);
        assert_eq!(key.flownode_id(), 1);
        assert_eq!(key.flow_id(), 2);
        assert_eq!(key.partition_id(), 0);
    }
}
