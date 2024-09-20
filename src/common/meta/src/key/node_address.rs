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

use api::v1::meta::Role;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error::{InvalidMetadataSnafu, Result};
use crate::key::{MetadataKey, NODE_ADDRESS_PATTERN, NODE_ADDRESS_PREFIX};
use crate::peer::Peer;

/// The key stores node address.
///
/// The layout: `__node_address/{role}/{node_id}`
#[derive(Debug, PartialEq)]
pub struct NodeAddressKey {
    pub role: Role,
    pub node_id: u64,
}

impl NodeAddressKey {
    pub fn new(role: Role, node_id: u64) -> Self {
        Self { role, node_id }
    }

    pub fn with_datanode(node_id: u64) -> Self {
        Self::new(Role::Datanode, node_id)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct NodeAddressValue {
    pub peer: Peer,
}

impl NodeAddressValue {
    pub fn new(peer: Peer) -> Self {
        Self { peer }
    }
}

impl<'a> MetadataKey<'a, NodeAddressKey> for NodeAddressKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<NodeAddressKey> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "NodeAddressKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures = NODE_ADDRESS_PATTERN
            .captures(key)
            .context(InvalidMetadataSnafu {
                err_msg: format!("Invalid NodeAddressKey '{key}'"),
            })?;
        // Safety: pass the regex check above
        let role = captures[1].parse::<i32>().unwrap();
        let role = Role::try_from(role).map_err(|_| {
            InvalidMetadataSnafu {
                err_msg: format!("Invalid Role value: {role}"),
            }
            .build()
        })?;
        let node_id = captures[2].parse::<u64>().unwrap();
        Ok(NodeAddressKey::new(role, node_id))
    }
}

impl Display for NodeAddressKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            NODE_ADDRESS_PREFIX, self.role as i32, self.node_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_address_key() {
        let key = NodeAddressKey::new(Role::Datanode, 1);
        let bytes = key.to_bytes();
        let key2 = NodeAddressKey::from_bytes(&bytes).unwrap();
        assert_eq!(key, key2);

        let key = NodeAddressKey::new(Role::Flownode, 3);
        let bytes = key.to_bytes();
        let key2 = NodeAddressKey::from_bytes(&bytes).unwrap();
        assert_eq!(key, key2);
    }
}
