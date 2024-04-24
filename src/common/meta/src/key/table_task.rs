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
use futures::StreamExt;
use snafu::OptionExt;
use table::metadata::TableId;

use crate::error::{self, Result};
use crate::key::{
    FlowTaskId, PartitionId, TableMetaKey, TABLE_TASK_KEY_PATTERN, TABLE_TASK_KEY_PREFIX,
};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;
use crate::FlownodeId;

/// The key of mapping [TableId] to [FlownodeId] and [TaskId].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableTaskKey {
    table_id: TableId,
    flownode_id: FlownodeId,
    task_id: FlowTaskId,
    partition_id: PartitionId,
}

impl TableTaskKey {
    /// Returns a new [TableTaskKey].
    pub fn new(
        table_id: TableId,
        flownode_id: FlownodeId,
        task_id: FlowTaskId,
        partition_id: PartitionId,
    ) -> TableTaskKey {
        Self {
            table_id,
            flownode_id,
            task_id,
            partition_id,
        }
    }

    fn prefix(table_id: TableId) -> String {
        format!("{}/{table_id}", TABLE_TASK_KEY_PREFIX)
    }

    /// The prefix used to retrieve all [TableTaskKey]s with the specified `table_id`.
    pub fn range_start_key(table_id: TableId) -> String {
        format!("{}/", Self::prefix(table_id))
    }

    /// Strips the [TaskId] from bytes.
    pub fn strip_key(raw_key: &[u8]) -> Result<TableTaskKey> {
        let key = String::from_utf8(raw_key.to_vec()).map_err(|e| {
            error::InvalidTableMetadataSnafu {
                err_msg: format!(
                    "FlownodeTaskKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(raw_key)
                ),
            }
            .build()
        })?;
        let captures =
            TABLE_TASK_KEY_PATTERN
                .captures(&key)
                .context(error::InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid FlownodeTaskKey '{key}'"),
                })?;
        // Safety: pass the regex check above
        let table_id = captures[1].parse::<TableId>().unwrap();
        let flownode_id = captures[2].parse::<FlownodeId>().unwrap();
        let task_id = captures[3].parse::<FlowTaskId>().unwrap();
        let partition_id = captures[4].parse::<PartitionId>().unwrap();
        Ok(TableTaskKey::new(
            table_id,
            flownode_id,
            task_id,
            partition_id,
        ))
    }
}

impl TableMetaKey for TableTaskKey {
    fn as_raw_key(&self) -> Vec<u8> {
        format!(
            "{TABLE_TASK_KEY_PREFIX}/{}/{}/{}/{}",
            self.table_id, self.flownode_id, self.task_id, self.partition_id
        )
        .into_bytes()
    }
}

/// Decodes `KeyValue` to ((),[TableTaskKey])
pub fn table_task_decoder(kv: KeyValue) -> Result<((), TableTaskKey)> {
    let key = TableTaskKey::strip_key(&kv.key)?;

    Ok(((), key))
}

/// The manager of [TableTaskKey].
pub struct TableTaskManager {
    kv_backend: KvBackendRef,
}

impl TableTaskManager {
    /// Returns a new [TableTaskManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Retrieves all ([FlownodeId], [TaskId])s of the specified `table_id`.
    pub fn nodes(&self, table_id: TableId) -> BoxStream<'static, Result<TableTaskKey>> {
        let start_key = TableTaskKey::range_start_key(table_id);
        let req = RangeRequest::new().with_prefix(start_key.as_bytes());

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(table_task_decoder),
        );

        Box::pin(stream.map(|kv| kv.map(|kv| kv.1)))
    }

    /// Builds a create table task transaction.
    /// 
    /// Puts `__table_task/{table_id}/{node_id}/{partition_id}` keys.
    pub fn build_create_txn<I: IntoIterator<Item = (PartitionId, FlownodeId)>>(
        &self,
        task_id: FlowTaskId,
        flownode_ids: I,
        source_table_ids: &[TableId],
    ) -> Txn {
        let txns = flownode_ids
            .into_iter()
            .flat_map(|(partition_id, flownode_id)| {
                source_table_ids.iter().map(move |table_id| {
                    TxnOp::Put(
                        TableTaskKey::new(*table_id, flownode_id, task_id, partition_id)
                            .as_raw_key(),
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
        let table_task_key = TableTaskKey::new(1024, 1, 2, 0);
        assert_eq!(
            b"__table_task/1024/1/2/0".to_vec(),
            table_task_key.as_raw_key()
        );
        let prefix = TableTaskKey::range_start_key(1024);
        assert_eq!("__table_task/1024/", &prefix);
    }

    #[test]
    fn test_strip_task_id() {
        let key = b"__table_task/1/10/100/0".to_vec();
        assert_eq!(
            TableTaskKey::new(1, 10, 100, 0),
            TableTaskKey::strip_key(&key).unwrap()
        );
    }
}
