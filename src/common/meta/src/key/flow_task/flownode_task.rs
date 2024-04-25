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
use futures::TryStreamExt;
use lazy_static::lazy_static;
use regex::Regex;
use snafu::OptionExt;

use crate::error::{self, Result};
use crate::key::flow_task::FlowTaskScoped;
use crate::key::scope::{BytesAdapter, CatalogScoped, MetaKey};
use crate::key::{FlowTaskId, FlowTaskPartitionId};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;
use crate::FlownodeId;

lazy_static! {
    static ref FLOWNODE_TASK_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{FLOWNODE_TASK_KEY_PREFIX}/([0-9]+)/([0-9]+)/([0-9]+)$"
    ))
    .unwrap();
}

const FLOWNODE_TASK_KEY_PREFIX: &str = "flownode";

/// The key of mapping [FlownodeId] to [FlowTaskId].
///
/// The layout `__flow_task/{catalog}/flownode/{flownode_id}/{flow_task_id}/{partition_id}`
pub struct FlownodeTaskKey(FlowTaskScoped<CatalogScoped<FlownodeTaskKeyInner>>);

impl MetaKey<FlownodeTaskKey> for FlownodeTaskKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlownodeTaskKey> {
        Ok(FlownodeTaskKey(FlowTaskScoped::<
            CatalogScoped<FlownodeTaskKeyInner>,
        >::from_bytes(bytes)?))
    }
}

impl FlownodeTaskKey {
    /// Returns a new [FlownodeTaskKey].
    pub fn new(
        catalog: String,
        flownode_id: FlownodeId,
        flow_task_id: FlowTaskId,
        partition_id: FlowTaskPartitionId,
    ) -> FlownodeTaskKey {
        let inner = FlownodeTaskKeyInner::new(flownode_id, flow_task_id, partition_id);
        FlownodeTaskKey(FlowTaskScoped::new(CatalogScoped::new(catalog, inner)))
    }

    /// The prefix used to retrieve all [FlownodeTaskKey]s with the specified `flownode_id`.
    pub fn range_start_key(catalog: String, flownode_id: FlownodeId) -> Vec<u8> {
        let catalog_scoped_key = CatalogScoped::new(
            catalog,
            BytesAdapter::from(FlownodeTaskKeyInner::range_start_key(flownode_id).into_bytes()),
        );

        FlowTaskScoped::new(catalog_scoped_key).to_bytes()
    }

    /// Returns the catalog.
    pub fn catalog(&self) -> &str {
        self.0.catalog()
    }

    /// Returns the [FlowTaskId].
    pub fn flow_task_id(&self) -> FlowTaskId {
        self.0.flow_task_id
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

/// The key of mapping [FlownodeId] to [FlowTaskId].
pub struct FlownodeTaskKeyInner {
    flownode_id: FlownodeId,
    flow_task_id: FlowTaskId,
    partition_id: FlowTaskPartitionId,
}

impl FlownodeTaskKeyInner {
    /// Returns a [FlownodeTaskKey] with the specified `flownode_id`, `flow_task_id` and `partition_id`.
    pub fn new(
        flownode_id: FlownodeId,
        flow_task_id: FlowTaskId,
        partition_id: FlowTaskPartitionId,
    ) -> Self {
        Self {
            flownode_id,
            flow_task_id,
            partition_id,
        }
    }

    fn prefix(flownode_id: FlownodeId) -> String {
        format!("{}/{flownode_id}", FLOWNODE_TASK_KEY_PREFIX)
    }

    /// The prefix used to retrieve all [FlownodeTaskKey]s with the specified `flownode_id`.
    fn range_start_key(flownode_id: FlownodeId) -> String {
        format!("{}/", Self::prefix(flownode_id))
    }
}

impl MetaKey<FlownodeTaskKeyInner> for FlownodeTaskKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!(
            "{FLOWNODE_TASK_KEY_PREFIX}/{}/{}/{}",
            self.flownode_id, self.flow_task_id, self.partition_id,
        )
        .into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlownodeTaskKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidTableMetadataSnafu {
                err_msg: format!(
                    "FlownodeTaskKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures =
            FLOWNODE_TASK_KEY_PATTERN
                .captures(key)
                .context(error::InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid FlownodeTaskKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let flownode_id = captures[1].parse::<FlownodeId>().unwrap();
        let flow_task_id = captures[2].parse::<FlowTaskId>().unwrap();
        let partition_id = captures[3].parse::<FlowTaskPartitionId>().unwrap();

        Ok(FlownodeTaskKeyInner {
            flownode_id,
            flow_task_id,
            partition_id,
        })
    }
}

/// The manager of [FlownodeTaskKey].
pub struct FlownodeTaskManager {
    kv_backend: KvBackendRef,
}

/// Decodes `KeyValue` to [FlownodeTaskKey].
pub fn flownode_task_key_decoder(kv: KeyValue) -> Result<FlownodeTaskKey> {
    FlownodeTaskKey::from_bytes(&kv.key)
}

impl FlownodeTaskManager {
    /// Returns a new [FlownodeTaskManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Retrieves all [FlowTaskId] and [PartitionId]s of the specified `flownode_id`.
    pub fn tasks(
        &self,
        catalog: &str,
        flownode_id: FlownodeId,
    ) -> BoxStream<'static, Result<(FlowTaskId, FlowTaskPartitionId)>> {
        let start_key = FlownodeTaskKey::range_start_key(catalog.to_string(), flownode_id);
        let req = RangeRequest::new().with_prefix(start_key);

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(flownode_task_key_decoder),
        );

        Box::pin(stream.map_ok(|key| (key.flow_task_id(), key.partition_id())))
    }

    /// Builds a create flownode task transaction.
    ///
    /// Puts `__flownode_task/{flownode_id}/{flow_task_id}/{partition_id}` keys.
    pub(crate) fn build_create_txn<I: IntoIterator<Item = (FlowTaskPartitionId, FlownodeId)>>(
        &self,
        catalog: &str,
        flow_task_id: FlowTaskId,
        flownode_ids: I,
    ) -> Txn {
        let txns = flownode_ids
            .into_iter()
            .map(|(partition_id, flownode_id)| {
                let key = FlownodeTaskKey::new(
                    catalog.to_string(),
                    flownode_id,
                    flow_task_id,
                    partition_id,
                )
                .to_bytes();
                TxnOp::Put(key, vec![])
            })
            .collect::<Vec<_>>();

        Txn::new().and_then(txns)
    }
}

#[cfg(test)]
mod tests {
    use crate::key::flow_task::flownode_task::FlownodeTaskKey;
    use crate::key::scope::MetaKey;

    #[test]
    fn test_key_serialization() {
        let flownode_task = FlownodeTaskKey::new("my_catalog".to_string(), 1, 2, 0);
        assert_eq!(
            b"__flow_task/my_catalog/flownode/1/2/0".to_vec(),
            flownode_task.to_bytes()
        );
        let prefix = FlownodeTaskKey::range_start_key("my_catalog".to_string(), 1);
        assert_eq!(b"__flow_task/my_catalog/flownode/1/".to_vec(), prefix);
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow_task/my_catalog/flownode/1/2/0".to_vec();
        let key = FlownodeTaskKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.catalog(), "my_catalog");
        assert_eq!(key.flownode_id(), 1);
        assert_eq!(key.flow_task_id(), 2);
        assert_eq!(key.partition_id(), 0);
    }
}
