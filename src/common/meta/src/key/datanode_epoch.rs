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

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::DatanodeId;
use crate::key::DATANODE_EPOCH_KEY_PREFIX;

/// The key stores the highest `node_epoch` the metasrv leader has accepted from a datanode.
///
/// The layout: `__datanode_epoch/{node_id}`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatanodeEpochKey {
    pub node_id: DatanodeId,
}

impl DatanodeEpochKey {
    pub fn new(node_id: DatanodeId) -> Self {
        Self { node_id }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

impl Display for DatanodeEpochKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", DATANODE_EPOCH_KEY_PREFIX, self.node_id)
    }
}

/// The value of [DatanodeEpochKey]. It only grows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatanodeEpochValue {
    pub node_epoch: u64,
}
