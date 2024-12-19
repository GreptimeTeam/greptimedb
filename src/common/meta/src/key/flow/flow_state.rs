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

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{self, Result};
use crate::key::flow::FlowScoped;
use crate::key::{FlowId, MetadataKey, MetadataValue};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::PutRequest;

/// The entire FlowId to Flow Size's Map is stored directly in the value part of the key.
const FLOW_STATE_KEY: &str = "state";

/// The key of flow state.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FlowStateKeyInner;

impl FlowStateKeyInner {
    pub fn new() -> Self {
        Self
    }
}

impl<'a> MetadataKey<'a, FlowStateKeyInner> for FlowStateKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        FLOW_STATE_KEY.as_bytes().to_vec()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowStateKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidMetadataSnafu {
                err_msg: format!(
                    "FlowInfoKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        if key != FLOW_STATE_KEY {
            return Err(error::InvalidMetadataSnafu {
                err_msg: format!("Invalid FlowStateKeyInner '{key}'"),
            }
            .build());
        }
        Ok(FlowStateKeyInner::new())
    }
}

/// The key stores the state size of the flow.
///
/// The layout: `__flow/state`.
pub struct FlowStateKey(FlowScoped<FlowStateKeyInner>);

impl FlowStateKey {
    /// Returns the [FlowStateKey].
    pub fn new() -> FlowStateKey {
        let inner = FlowStateKeyInner::new();
        FlowStateKey(FlowScoped::new(inner))
    }
}

impl Default for FlowStateKey {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> MetadataKey<'a, FlowStateKey> for FlowStateKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowStateKey> {
        Ok(FlowStateKey(FlowScoped::<FlowStateKeyInner>::from_bytes(
            bytes,
        )?))
    }
}

/// The value of flow state size
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlowStateValue {
    /// For each key, the bytes of the state in memory
    pub state_size: BTreeMap<FlowId, usize>,
}

impl FlowStateValue {
    pub fn new(state_size: BTreeMap<FlowId, usize>) -> Self {
        Self { state_size }
    }
}

pub type FlowStateManagerRef = Arc<FlowStateManager>;

/// The manager of [FlowStateKey]. Since state size changes frequently, we store it in memory.
///
/// This is only used in distributed mode. When meta-srv use heartbeat to update the flow stat report
/// and frontned use get to get the latest flow stat report.
pub struct FlowStateManager {
    in_memory: KvBackendRef,
}

impl FlowStateManager {
    pub fn new(in_memory: KvBackendRef) -> Self {
        Self { in_memory }
    }

    pub async fn get(&self) -> Result<Option<FlowStateValue>> {
        let key = FlowStateKey::new().to_bytes();
        self.in_memory
            .get(&key)
            .await?
            .map(|x| FlowStateValue::try_from_raw_value(&x.value))
            .transpose()
    }

    pub async fn put(&self, value: FlowStateValue) -> Result<()> {
        let key = FlowStateKey::new().to_bytes();
        let value = value.try_as_raw_value()?;
        let req = PutRequest::new().with_key(key).with_value(value);
        self.in_memory.put(req).await?;
        Ok(())
    }
}

/// Flow's state report, send regularly through heartbeat message
#[derive(Debug, Clone)]
pub struct FlowStat {
    /// For each key, the bytes of the state in memory
    pub state_size: BTreeMap<u32, usize>,
}

impl From<FlowStateValue> for FlowStat {
    fn from(value: FlowStateValue) -> Self {
        Self {
            state_size: value.state_size,
        }
    }
}

impl From<FlowStat> for FlowStateValue {
    fn from(value: FlowStat) -> Self {
        Self {
            state_size: value.state_size,
        }
    }
}
