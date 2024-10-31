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
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error::{self, Result};
use crate::key::flow::FlowScoped;
use crate::key::{BytesAdapter, FlowId, FlowPartitionId, MetadataKey, MetadataValue};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::peer::Peer;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;

const FLOW_ROUTE_KEY_PREFIX: &str = "route";

lazy_static! {
    static ref FLOW_ROUTE_KEY_PATTERN: Regex =
        Regex::new(&format!("^{FLOW_ROUTE_KEY_PREFIX}/([0-9]+)/([0-9]+)$")).unwrap();
}

/// The key stores the route info of the flow.
///
/// The layout: `__flow/route/{flow_id}/{partition_id}`.
#[derive(Debug, PartialEq)]
pub struct FlowRouteKey(FlowScoped<FlowRouteKeyInner>);

impl FlowRouteKey {
    /// Returns a new [FlowRouteKey].
    pub fn new(flow_id: FlowId, partition_id: FlowPartitionId) -> FlowRouteKey {
        let inner = FlowRouteKeyInner::new(flow_id, partition_id);
        FlowRouteKey(FlowScoped::new(inner))
    }

    /// The prefix used to retrieve all [FlowRouteKey]s with the specified `flow_id`.
    pub fn range_start_key(flow_id: FlowId) -> Vec<u8> {
        let inner = BytesAdapter::from(FlowRouteKeyInner::prefix(flow_id).into_bytes());

        FlowScoped::new(inner).to_bytes()
    }

    /// Returns the [`FlowId`]
    pub fn flow_id(&self) -> FlowId {
        self.0.flow_id
    }

    /// Returns the [`FlowPartitionId`]
    pub fn partition_id(&self) -> FlowPartitionId {
        self.0.partition_id
    }
}

impl<'a> MetadataKey<'a, FlowRouteKey> for FlowRouteKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowRouteKey> {
        Ok(FlowRouteKey(FlowScoped::<FlowRouteKeyInner>::from_bytes(
            bytes,
        )?))
    }
}

/// The key of flow route metadata.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FlowRouteKeyInner {
    flow_id: FlowId,
    partition_id: FlowPartitionId,
}

impl FlowRouteKeyInner {
    /// Returns a [FlowRouteKeyInner] with the specified `flow_id` and `partition_id`.
    pub fn new(flow_id: FlowId, partition_id: FlowPartitionId) -> FlowRouteKeyInner {
        FlowRouteKeyInner {
            flow_id,
            partition_id,
        }
    }

    fn prefix(flow_id: FlowId) -> String {
        format!("{}/{flow_id}/", FLOW_ROUTE_KEY_PREFIX)
    }
}

impl<'a> MetadataKey<'a, FlowRouteKeyInner> for FlowRouteKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!(
            "{FLOW_ROUTE_KEY_PREFIX}/{}/{}",
            self.flow_id, self.partition_id
        )
        .into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowRouteKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidMetadataSnafu {
                err_msg: format!(
                    "FlowInfoKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures =
            FLOW_ROUTE_KEY_PATTERN
                .captures(key)
                .context(error::InvalidMetadataSnafu {
                    err_msg: format!("Invalid FlowInfoKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let flow_id = captures[1].parse::<FlowId>().unwrap();
        let partition_id = captures[2].parse::<FlowId>().unwrap();

        Ok(FlowRouteKeyInner {
            flow_id,
            partition_id,
        })
    }
}

/// The route info of flow.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlowRouteValue {
    pub(crate) peer: Peer,
}

impl FlowRouteValue {
    /// Returns the `peer`.
    pub fn peer(&self) -> &Peer {
        &self.peer
    }
}

/// Decodes `KeyValue` to ([`FlowRouteKey`],[`FlowRouteValue`]).
pub fn flow_route_decoder(kv: KeyValue) -> Result<(FlowRouteKey, FlowRouteValue)> {
    let key = FlowRouteKey::from_bytes(&kv.key)?;
    let value = FlowRouteValue::try_from_raw_value(&kv.value)?;
    Ok((key, value))
}

/// The manager of [FlowRouteKey].
pub struct FlowRouteManager {
    kv_backend: KvBackendRef,
}

impl FlowRouteManager {
    /// Returns a new [FlowRouteManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Retrieves all [FlowRouteValue]s of the specified `flow_id`.
    pub fn routes(
        &self,
        flow_id: FlowId,
    ) -> BoxStream<'static, Result<(FlowRouteKey, FlowRouteValue)>> {
        let start_key = FlowRouteKey::range_start_key(flow_id);
        let req = RangeRequest::new().with_prefix(start_key);
        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(flow_route_decoder),
        )
        .into_stream();

        Box::pin(stream)
    }

    /// Builds a create flow routes transaction.
    ///
    /// Puts `__flow/route/{flow_id}/{partition_id}` keys.
    pub(crate) fn build_create_txn<I: IntoIterator<Item = (FlowPartitionId, FlowRouteValue)>>(
        &self,
        flow_id: FlowId,
        flow_routes: I,
    ) -> Result<Txn> {
        let txns = flow_routes
            .into_iter()
            .map(|(partition_id, route)| {
                let key = FlowRouteKey::new(flow_id, partition_id).to_bytes();

                Ok(TxnOp::Put(key, route.try_as_raw_value()?))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Txn::new().and_then(txns))
    }
}

#[cfg(test)]
mod tests {
    use super::FlowRouteKey;
    use crate::key::MetadataKey;

    #[test]
    fn test_key_serialization() {
        let flow_route_key = FlowRouteKey::new(1, 2);
        assert_eq!(b"__flow/route/1/2".to_vec(), flow_route_key.to_bytes());
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow/route/1/2".to_vec();
        let key = FlowRouteKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.flow_id(), 1);
        assert_eq!(key.partition_id(), 2);
    }

    #[test]
    fn test_key_start_range() {
        assert_eq!(
            b"__flow/route/2/".to_vec(),
            FlowRouteKey::range_start_key(2)
        );
    }
}
