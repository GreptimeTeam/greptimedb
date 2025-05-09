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

mod datanode;
mod flownode;

use std::str::FromStr;

use api::v1::meta::heartbeat_request::NodeWorkloads;
pub use datanode::*;
pub use flownode::*;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error;

macro_rules! impl_from_str_lease_key {
    ($key_type:ty, $pattern:expr) => {
        impl FromStr for $key_type {
            type Err = error::Error;

            fn from_str(key: &str) -> error::Result<Self> {
                let caps = $pattern
                    .captures(key)
                    .context(error::InvalidLeaseKeySnafu { key })?;

                ensure!(caps.len() == 3, error::InvalidLeaseKeySnafu { key });
                let node_id = caps[2].to_string();
                let node_id: u64 = node_id.parse().context(error::ParseNumSnafu {
                    err_msg: format!("invalid node_id: {node_id}"),
                })?;

                Ok(Self { node_id })
            }
        }
    };
}

impl_from_str_lease_key!(FlownodeLeaseKey, FLOWNODE_LEASE_KEY_PATTERN);
impl_from_str_lease_key!(DatanodeLeaseKey, DATANODE_LEASE_KEY_PATTERN);

macro_rules! impl_try_from_lease_key {
    ($key_type:ty, $prefix:expr) => {
        impl TryFrom<Vec<u8>> for $key_type {
            type Error = error::Error;

            fn try_from(bytes: Vec<u8>) -> error::Result<Self> {
                String::from_utf8(bytes)
                    .context(error::LeaseKeyFromUtf8Snafu {})
                    .map(|x| x.parse())?
            }
        }

        impl TryFrom<$key_type> for Vec<u8> {
            type Error = error::Error;

            fn try_from(key: $key_type) -> error::Result<Self> {
                Ok(format!("{}-0-{}", $prefix, key.node_id).into_bytes())
            }
        }
    };
}

impl_try_from_lease_key!(FlownodeLeaseKey, FLOWNODE_LEASE_PREFIX);
impl_try_from_lease_key!(DatanodeLeaseKey, DATANODE_LEASE_PREFIX);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaseValue {
    // last activity
    pub timestamp_millis: i64,
    pub node_addr: String,
    pub workloads: NodeWorkloads,
}

impl FromStr for LeaseValue {
    type Err = error::Error;

    fn from_str(value: &str) -> crate::Result<Self> {
        serde_json::from_str(value).context(error::DeserializeFromJsonSnafu { input: value })
    }
}

impl TryFrom<Vec<u8>> for LeaseValue {
    type Error = error::Error;

    fn try_from(bytes: Vec<u8>) -> crate::Result<Self> {
        String::from_utf8(bytes)
            .context(error::LeaseValueFromUtf8Snafu {})
            .map(|x| x.parse())?
    }
}

impl TryFrom<LeaseValue> for Vec<u8> {
    type Error = error::Error;

    fn try_from(value: LeaseValue) -> crate::Result<Self> {
        Ok(serde_json::to_string(&value)
            .context(error::SerializeToJsonSnafu {
                input: format!("{value:?}"),
            })?
            .into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::DatanodeWorkloads;

    use super::*;

    #[test]
    fn test_lease_value_round_trip() {
        let value = LeaseValue {
            timestamp_millis: 111,
            node_addr: "127.0.0.1:3002".to_string(),
            workloads: NodeWorkloads::Datanode(DatanodeWorkloads { types: vec![] }),
        };

        let value_bytes: Vec<u8> = value.clone().try_into().unwrap();
        let new_value: LeaseValue = value_bytes.try_into().unwrap();

        assert_eq!(new_value, value);
    }
}
