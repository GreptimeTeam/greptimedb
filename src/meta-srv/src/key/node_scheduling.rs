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

use std::str::FromStr;

use snafu::OptionExt;

use crate::error;
use crate::error::Result;

/// Key prefix of datanode scheduling states.
const DATANODE_SCHEDULING_PREFIX: &str = "__node_scheduling/datanode/";
/// Key prefix of flownode scheduling states.
const FLOWNODE_SCHEDULING_PREFIX: &str = "__node_scheduling/flownode/";

/// The node roles whose scheduling state is tracked.
///
/// A subset of [`common_meta::cluster::Role`]: frontends and metasrv nodes never receive
/// regions, so they are out of scope.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum NodeSchedulingRole {
    Datanode,
    Flownode,
}

impl NodeSchedulingRole {
    /// Returns the key prefix of this role, e.g. `__node_scheduling/datanode/`.
    fn key_prefix(&self) -> &'static str {
        match self {
            NodeSchedulingRole::Datanode => DATANODE_SCHEDULING_PREFIX,
            NodeSchedulingRole::Flownode => FLOWNODE_SCHEDULING_PREFIX,
        }
    }
}

/// Key of a node's scheduling state: `__node_scheduling/{role}/{node_id}`,
/// for example `__node_scheduling/datanode/7`.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct NodeSchedulingKey {
    pub role: NodeSchedulingRole,
    pub node_id: u64,
}

impl NodeSchedulingKey {
    pub fn new(role: NodeSchedulingRole, node_id: u64) -> NodeSchedulingKey {
        NodeSchedulingKey { role, node_id }
    }

    /// Returns the prefix covering every node of `role`.
    pub fn prefix_key(role: NodeSchedulingRole) -> Vec<u8> {
        role.key_prefix().as_bytes().to_vec()
    }
}

impl From<NodeSchedulingKey> for Vec<u8> {
    fn from(key: NodeSchedulingKey) -> Vec<u8> {
        format!("{}{}", key.role.key_prefix(), key.node_id).into_bytes()
    }
}

impl FromStr for NodeSchedulingKey {
    type Err = error::Error;

    fn from_str(key: &str) -> Result<NodeSchedulingKey> {
        for role in [NodeSchedulingRole::Datanode, NodeSchedulingRole::Flownode] {
            let Some(node_id) = key.strip_prefix(role.key_prefix()) else {
                continue;
            };

            let node_id = node_id
                .parse::<u64>()
                .ok()
                .context(error::InvalidNodeSchedulingKeySnafu { key })?;
            return Ok(NodeSchedulingKey { role, node_id });
        }

        error::InvalidNodeSchedulingKeySnafu { key }.fail()
    }
}

impl TryFrom<Vec<u8>> for NodeSchedulingKey {
    type Error = error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<NodeSchedulingKey> {
        let key = String::from_utf8(bytes).map_err(|err| {
            error::InvalidNodeSchedulingKeySnafu {
                key: String::from_utf8_lossy(err.as_bytes()).into_owned(),
            }
            .build()
        })?;
        key.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_round_trip() {
        for (role, expected) in [
            (NodeSchedulingRole::Datanode, "__node_scheduling/datanode/7"),
            (NodeSchedulingRole::Flownode, "__node_scheduling/flownode/7"),
        ] {
            let key = NodeSchedulingKey::new(role, 7);
            let bytes: Vec<u8> = key.into();
            assert_eq!(expected.as_bytes(), bytes.as_slice());

            let parsed = NodeSchedulingKey::try_from(bytes).unwrap();
            assert_eq!(key, parsed);
            assert_eq!(role, parsed.role);
        }
    }

    #[test]
    fn test_key_prefix() {
        assert_eq!(
            b"__node_scheduling/datanode/",
            NodeSchedulingKey::prefix_key(NodeSchedulingRole::Datanode).as_slice()
        );
        assert_eq!(
            b"__node_scheduling/flownode/",
            NodeSchedulingKey::prefix_key(NodeSchedulingRole::Flownode).as_slice()
        );
    }

    #[test]
    fn test_invalid_key() {
        for key in [
            "__node_scheduling/frontend/7",
            "__node_scheduling/datanode/",
            "__node_scheduling/datanode/x",
            "__node_scheduling",
        ] {
            let err = key.parse::<NodeSchedulingKey>().unwrap_err();
            assert!(
                matches!(err, error::Error::InvalidNodeSchedulingKey { .. }),
                "unexpected error for {key}: {err:?}"
            );
        }
    }
}
