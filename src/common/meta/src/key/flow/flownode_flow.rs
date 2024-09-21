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
use crate::key::flow::FlowScoped;
use crate::key::{BytesAdapter, FlowId, FlowPartitionId, MetadataKey};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;
use crate::FlownodeId;

lazy_static! {
    static ref FLOWNODE_FLOW_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{FLOWNODE_FLOW_KEY_PREFIX}/([0-9]+)/([0-9]+)/([0-9]+)$"
    ))
    .unwrap();
}

const FLOWNODE_FLOW_KEY_PREFIX: &str = "flownode";

/// The key of mapping [FlownodeId] to [FlowId].
///
/// The layout `__flow/flownode/{flownode_id}/{flow_id}/{partition_id}`
pub struct FlownodeFlowKey(FlowScoped<FlownodeFlowKeyInner>);

impl<'a> MetadataKey<'a, FlownodeFlowKey> for FlownodeFlowKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlownodeFlowKey> {
        Ok(FlownodeFlowKey(
            FlowScoped::<FlownodeFlowKeyInner>::from_bytes(bytes)?,
        ))
    }
}

impl FlownodeFlowKey {
    /// Returns a new [FlownodeFlowKey].
    pub fn new(
        flownode_id: FlownodeId,
        flow_id: FlowId,
        partition_id: FlowPartitionId,
    ) -> FlownodeFlowKey {
        let inner = FlownodeFlowKeyInner::new(flownode_id, flow_id, partition_id);
        FlownodeFlowKey(FlowScoped::new(inner))
    }

    /// The prefix used to retrieve all [FlownodeFlowKey]s with the specified `flownode_id`.
    pub fn range_start_key(flownode_id: FlownodeId) -> Vec<u8> {
        let inner = BytesAdapter::from(FlownodeFlowKeyInner::prefix(flownode_id).into_bytes());

        FlowScoped::new(inner).to_bytes()
    }

    /// Returns the [FlowId].
    pub fn flow_id(&self) -> FlowId {
        self.0.flow_id
    }

    #[cfg(test)]
    /// Returns the [FlownodeId].
    pub fn flownode_id(&self) -> FlownodeId {
        self.0.flownode_id
    }

    /// Returns the [PartitionId].
    pub fn partition_id(&self) -> FlowPartitionId {
        self.0.partition_id
    }
}

/// The key of mapping [FlownodeId] to [FlowId].
pub struct FlownodeFlowKeyInner {
    flownode_id: FlownodeId,
    flow_id: FlowId,
    partition_id: FlowPartitionId,
}

impl FlownodeFlowKeyInner {
    /// Returns a [FlownodeFlowKey] with the specified `flownode_id`, `flow_id` and `partition_id`.
    pub fn new(flownode_id: FlownodeId, flow_id: FlowId, partition_id: FlowPartitionId) -> Self {
        Self {
            flownode_id,
            flow_id,
            partition_id,
        }
    }

    pub fn prefix(flownode_id: FlownodeId) -> String {
        format!("{}/{flownode_id}/", FLOWNODE_FLOW_KEY_PREFIX)
    }
}

impl<'a> MetadataKey<'a, FlownodeFlowKeyInner> for FlownodeFlowKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!(
            "{FLOWNODE_FLOW_KEY_PREFIX}/{}/{}/{}",
            self.flownode_id, self.flow_id, self.partition_id,
        )
        .into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlownodeFlowKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidMetadataSnafu {
                err_msg: format!(
                    "FlownodeFlowKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures =
            FLOWNODE_FLOW_KEY_PATTERN
                .captures(key)
                .context(error::InvalidMetadataSnafu {
                    err_msg: format!("Invalid FlownodeFlowKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let flownode_id = captures[1].parse::<FlownodeId>().unwrap();
        let flow_id = captures[2].parse::<FlowId>().unwrap();
        let partition_id = captures[3].parse::<FlowPartitionId>().unwrap();

        Ok(FlownodeFlowKeyInner {
            flownode_id,
            flow_id,
            partition_id,
        })
    }
}

/// The manager of [FlownodeFlowKey].
pub struct FlownodeFlowManager {
    kv_backend: KvBackendRef,
}

/// Decodes `KeyValue` to [FlownodeFlowKey].
pub fn flownode_flow_key_decoder(kv: KeyValue) -> Result<FlownodeFlowKey> {
    FlownodeFlowKey::from_bytes(&kv.key)
}

impl FlownodeFlowManager {
    /// Returns a new [FlownodeFlowManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Retrieves all [FlowId] and [FlowPartitionId]s of the specified `flownode_id`.
    pub fn flows(
        &self,
        flownode_id: FlownodeId,
    ) -> BoxStream<'static, Result<(FlowId, FlowPartitionId)>> {
        let start_key = FlownodeFlowKey::range_start_key(flownode_id);
        let req = RangeRequest::new().with_prefix(start_key);

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(flownode_flow_key_decoder),
        )
        .into_stream();

        Box::pin(stream.map_ok(|key| (key.flow_id(), key.partition_id())))
    }

    /// Builds a create flownode flow transaction.
    ///
    /// Puts `__flownode_flow/{flownode_id}/{flow_id}/{partition_id}` keys.
    pub(crate) fn build_create_txn<I: IntoIterator<Item = (FlowPartitionId, FlownodeId)>>(
        &self,
        flow_id: FlowId,
        flownode_ids: I,
    ) -> Txn {
        let txns = flownode_ids
            .into_iter()
            .map(|(partition_id, flownode_id)| {
                let key = FlownodeFlowKey::new(flownode_id, flow_id, partition_id).to_bytes();
                TxnOp::Put(key, vec![])
            })
            .collect::<Vec<_>>();

        Txn::new().and_then(txns)
    }
}

#[cfg(test)]
mod tests {
    use crate::key::flow::flownode_flow::FlownodeFlowKey;
    use crate::key::MetadataKey;

    #[test]
    fn test_key_serialization() {
        let flownode_flow = FlownodeFlowKey::new(1, 2, 0);
        assert_eq!(b"__flow/flownode/1/2/0".to_vec(), flownode_flow.to_bytes());
        let prefix = FlownodeFlowKey::range_start_key(1);
        assert_eq!(b"__flow/flownode/1/".to_vec(), prefix);
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow/flownode/1/2/0".to_vec();
        let key = FlownodeFlowKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.flownode_id(), 1);
        assert_eq!(key.flow_id(), 2);
        assert_eq!(key.partition_id(), 0);
    }
}
