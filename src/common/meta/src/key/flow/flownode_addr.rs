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
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error::{self, Result};
use crate::key::flow::FlowScoped;
use crate::key::{BytesAdapter, FlowId, FlowPartitionId, MetaKey, TableMetaValue};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;
use crate::FlownodeId;

lazy_static! {
    static ref FLOWNODE_FLOW_KEY_PATTERN: Regex =
        Regex::new(&format!("^{FLOWNODE_ADDR_KEY_PREFIX}/([0-9]+)$")).unwrap();
}

const FLOWNODE_ADDR_KEY_PREFIX: &str = "addr";

/// The key of mapping [FlownodeId] to address.
///
/// The layout `__flow/addr/{flownode_id}`
pub struct FlownodeAddrKey(FlowScoped<FlownodeAddrKeyInner>);

impl FlownodeAddrKey {
    /// Returns a new [FlownodeAddrKey].
    pub fn new(flownode_id: FlownodeId) -> FlownodeAddrKey {
        let inner = FlownodeAddrKeyInner { flownode_id };
        FlownodeAddrKey(FlowScoped::new(inner))
    }

    /// Returns the [FlownodeId].
    pub fn flownode_id(&self) -> FlownodeId {
        self.0.flownode_id
    }
}

impl<'a> MetaKey<'a, FlownodeAddrKey> for FlownodeAddrKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlownodeAddrKey> {
        Ok(FlownodeAddrKey(
            FlowScoped::<FlownodeAddrKeyInner>::from_bytes(bytes)?,
        ))
    }
}

/// The key of mapping [FlownodeId] to [FlowId].
pub struct FlownodeAddrKeyInner {
    flownode_id: FlownodeId,
}

impl<'a> MetaKey<'a, FlownodeAddrKeyInner> for FlownodeAddrKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!("{FLOWNODE_ADDR_KEY_PREFIX}/{}", self.flownode_id).into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlownodeAddrKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidTableMetadataSnafu {
                err_msg: format!(
                    "FlownodeAddrKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures =
            FLOWNODE_FLOW_KEY_PATTERN
                .captures(key)
                .context(error::InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid FlownodeAddrKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let flownode_id = captures[1].parse::<FlownodeId>().unwrap();
        Ok(FlownodeAddrKeyInner { flownode_id })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlownodeAddrValue {
    pub addr: String,
}

impl FlownodeAddrValue {
    pub fn new(addr: String) -> Self {
        Self { addr }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }
}

pub type FlownodeAddrManagerRef = Arc<FlownodeAddrManager>;

pub struct FlownodeAddrManager {
    kv_backend: KvBackendRef,
}

impl FlownodeAddrManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Return the address of the flownode.
    pub async fn get(&self, flownode_id: FlownodeId) -> Result<Option<FlownodeAddrValue>> {
        let key = FlownodeAddrKey::new(flownode_id).to_bytes();
        self.kv_backend
            .get(&key)
            .await?
            .map(|x| FlownodeAddrValue::try_from_raw_value(&x.value))
            .transpose()
    }
}
