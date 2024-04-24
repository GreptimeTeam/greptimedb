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

use crate::error::{self, Result};
use crate::key::{
    FlowTaskId, PartitionId, TableMetaKey, FLOWNODE_TASK_KEY_PATTERN, FLOWNODE_TASK_KEY_PREFIX,
};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;
use crate::FlownodeId;

/// The key of mapping [FlownodeId] to [TaskId].
pub struct FlownodeTaskKey {
    flownode_id: FlownodeId,
    task_id: FlowTaskId,
    partition_id: PartitionId,
}

impl FlownodeTaskKey {
    /// Returns a [FlownodeTaskKey] with the specified `flow_node` and `task_id`.
    pub fn new(flownode_id: FlownodeId, task_id: FlowTaskId, partition_id: PartitionId) -> Self {
        Self {
            flownode_id,
            task_id,
            partition_id,
        }
    }

    fn prefix(flownode_id: FlownodeId) -> String {
        format!("{}/{flownode_id}", FLOWNODE_TASK_KEY_PREFIX)
    }

    /// The prefix used to retrieve all [FlownodeTaskKey]s with the specified `flownode_id`.
    pub fn range_start_key(flownode_id: FlownodeId) -> String {
        format!("{}/", Self::prefix(flownode_id))
    }

    /// Strips the [TaskId] from bytes.
    pub fn strip_task_id_and_partition_id(raw_key: &[u8]) -> Result<(FlowTaskId, PartitionId)> {
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
            FLOWNODE_TASK_KEY_PATTERN
                .captures(&key)
                .context(error::InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid FlownodeTaskKey '{key}'"),
                })?;
        // Safety: pass the regex check above
        let task_id = captures[2].parse::<FlowTaskId>().unwrap();
        let partition_id = captures[3].parse::<PartitionId>().unwrap();
        Ok((task_id, partition_id))
    }
}

impl TableMetaKey for FlownodeTaskKey {
    fn as_raw_key(&self) -> Vec<u8> {
        format!(
            "{FLOWNODE_TASK_KEY_PREFIX}/{}/{}/{}",
            self.flownode_id, self.task_id, self.partition_id,
        )
        .into_bytes()
    }
}

/// The manager of [FlownodeTaskKey].
pub struct FlownodeTaskManager {
    kv_backend: KvBackendRef,
}

/// Decodes `KeyValue` to ((),[TaskId])
pub fn flownode_task_key_decoder(kv: KeyValue) -> Result<(FlowTaskId, PartitionId)> {
    FlownodeTaskKey::strip_task_id_and_partition_id(&kv.key)
}

impl FlownodeTaskManager {
    /// Returns a new [FlownodeTaskManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Retrieves all [TaskId]s of the specified `flownode_id`.
    pub fn tasks(
        &self,
        flownode_id: FlownodeId,
    ) -> BoxStream<'static, Result<(FlowTaskId, PartitionId)>> {
        let start_key = FlownodeTaskKey::range_start_key(flownode_id);
        let req = RangeRequest::new().with_prefix(start_key.as_bytes());

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(flownode_task_key_decoder),
        );

        Box::pin(stream)
    }

    /// Builds a create flownode task transaction.
    pub(crate) fn build_create_txn<I: IntoIterator<Item = (PartitionId, FlownodeId)>>(
        &self,
        task_id: FlowTaskId,
        flownode_ids: I,
    ) -> Txn {
        let txns = flownode_ids
            .into_iter()
            .map(|(partition_id, flownode_id)| {
                let key = FlownodeTaskKey::new(flownode_id, task_id, partition_id);
                let raw_key = key.as_raw_key();
                TxnOp::Put(raw_key.clone(), vec![])
            })
            .collect::<Vec<_>>();

        Txn::new().and_then(txns)
    }
}

#[cfg(test)]
mod tests {
    use super::FlownodeTaskKey;
    use crate::key::TableMetaKey;

    #[test]
    fn test_key_serialization() {
        let flownode_task = FlownodeTaskKey::new(1, 2, 0);
        assert_eq!(
            b"__flownode_task/1/2/0".to_vec(),
            flownode_task.as_raw_key()
        );
        let prefix = FlownodeTaskKey::range_start_key(1);
        assert_eq!("__flownode_task/1/", &prefix);
    }

    #[test]
    fn test_strip_task_id_and_partition_id() {
        let key = b"__flownode_task/1/10/0".to_vec();
        assert_eq!(
            (10, 0),
            FlownodeTaskKey::strip_task_id_and_partition_id(&key).unwrap()
        );
    }
}
