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
use snafu::OptionExt;
use table::metadata::TableId;

use super::TABLE_TASK_KEY_PATTERN;
use crate::error::{self, Result};
use crate::key::{TableMetaKey, TaskId, TABLE_TASK_KEY_PREFIX};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;
use crate::FlownodeId;

/// The key of mapping [TableId] to [FlownodeId] and [TaskId].
pub struct TableTaskKey {
    table_id: TableId,
    flownode_id: FlownodeId,
    task_id: TaskId,
}

impl TableTaskKey {
    /// Returns a new [TableTaskKey].
    pub fn new(table_id: TableId, flownode_id: FlownodeId, task_id: TaskId) -> TableTaskKey {
        Self {
            table_id,
            flownode_id,
            task_id,
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
    pub fn strip_flownode_id_and_task_id(raw_key: &[u8]) -> Result<(FlownodeId, TaskId)> {
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
        let flownode_id = captures[2].parse::<FlownodeId>().unwrap();
        let task_id = captures[3].parse::<TaskId>().unwrap();
        Ok((flownode_id, task_id))
    }
}

impl TableMetaKey for TableTaskKey {
    fn as_raw_key(&self) -> Vec<u8> {
        format!(
            "{}/{}/{}/{}",
            TABLE_TASK_KEY_PREFIX, self.table_id, self.flownode_id, self.task_id
        )
        .into_bytes()
    }
}

/// Decodes `KeyValue` to ((),[TaskId])
pub fn table_task_decoder(kv: KeyValue) -> Result<(FlownodeId, TaskId)> {
    TableTaskKey::strip_flownode_id_and_task_id(&kv.key)
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
    pub fn nodes(&self, table_id: TableId) -> BoxStream<'static, Result<(FlownodeId, TaskId)>> {
        let start_key = TableTaskKey::range_start_key(table_id);
        let req = RangeRequest::new().with_prefix(start_key.as_bytes());

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(table_task_decoder),
        );

        Box::pin(stream)
    }

    /// Builds a create table task transaction.
    pub fn build_create_txn(
        &self,
        task_id: TaskId,
        flownode_id: FlownodeId,
        source_table_ids: &[TableId],
    ) -> Txn {
        let txns = source_table_ids
            .iter()
            .map(|table_id| {
                TxnOp::Put(
                    TableTaskKey::new(*table_id, flownode_id, task_id).as_raw_key(),
                    vec![],
                )
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
        let table_task_key = TableTaskKey::new(1024, 1, 2);
        assert_eq!(
            b"__table_task/1024/1/2".to_vec(),
            table_task_key.as_raw_key()
        );
        let prefix = TableTaskKey::range_start_key(1024);
        assert_eq!("__table_task/1024/", &prefix);
    }

    #[test]
    fn test_strip_task_id() {
        let key = b"__table_task/1/10/100".to_vec();
        assert_eq!(
            (10, 100),
            TableTaskKey::strip_flownode_id_and_task_id(&key).unwrap()
        );
    }
}
