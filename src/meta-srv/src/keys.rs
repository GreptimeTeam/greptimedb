// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str::FromStr;

use api::v1::meta::TableName;
use common_catalog::TableGlobalKey;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error;
use crate::error::Result;

pub(crate) const DN_LEASE_PREFIX: &str = "__meta_dnlease";
pub(crate) const SEQ_PREFIX: &str = "__meta_seq";
pub(crate) const TABLE_ROUTE_PREFIX: &str = "__meta_table_route";

lazy_static! {
    static ref DATANODE_KEY_PATTERN: Regex =
        Regex::new(&format!("^{}-([0-9]+)-([0-9]+)$", DN_LEASE_PREFIX)).unwrap();
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LeaseKey {
    pub cluster_id: u64,
    pub node_id: u64,
}

impl FromStr for LeaseKey {
    type Err = error::Error;

    fn from_str(key: &str) -> Result<Self> {
        let caps = DATANODE_KEY_PATTERN
            .captures(key)
            .context(error::InvalidLeaseKeySnafu { key })?;

        ensure!(caps.len() == 3, error::InvalidLeaseKeySnafu { key });

        let cluster_id = caps[1].to_string();
        let node_id = caps[2].to_string();
        let cluster_id: u64 = cluster_id.parse().context(error::ParseNumSnafu {
            err_msg: format!("invalid cluster_id: {}", cluster_id),
        })?;
        let node_id: u64 = node_id.parse().context(error::ParseNumSnafu {
            err_msg: format!("invalid node_id: {}", node_id),
        })?;

        Ok(Self {
            cluster_id,
            node_id,
        })
    }
}

impl TryFrom<Vec<u8>> for LeaseKey {
    type Error = error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(error::LeaseKeyFromUtf8Snafu {})
            .map(|x| x.parse())?
    }
}

impl TryFrom<LeaseKey> for Vec<u8> {
    type Error = error::Error;

    fn try_from(dn_key: LeaseKey) -> Result<Self> {
        Ok(format!(
            "{}-{}-{}",
            DN_LEASE_PREFIX, dn_key.cluster_id, dn_key.node_id
        )
        .into_bytes())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaseValue {
    // last activity
    pub timestamp_millis: i64,
    pub node_addr: String,
}

impl FromStr for LeaseValue {
    type Err = error::Error;

    fn from_str(value: &str) -> Result<Self> {
        serde_json::from_str(value).context(error::DeserializeFromJsonSnafu { input: value })
    }
}

impl TryFrom<Vec<u8>> for LeaseValue {
    type Error = error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(error::LeaseKeyFromUtf8Snafu {})
            .map(|x| x.parse())?
    }
}

impl TryFrom<LeaseValue> for Vec<u8> {
    type Error = error::Error;

    fn try_from(dn_value: LeaseValue) -> Result<Self> {
        Ok(serde_json::to_string(&dn_value)
            .context(error::SerializeToJsonSnafu {
                input: format!("{:?}", dn_value),
            })?
            .into_bytes())
    }
}

pub struct TableRouteKey<'a> {
    pub table_id: u64,
    pub catalog_name: &'a str,
    pub schema_name: &'a str,
    pub table_name: &'a str,
}

impl<'a> TableRouteKey<'a> {
    pub fn with_table_name(table_id: u64, t: &'a TableName) -> Self {
        Self {
            table_id,
            catalog_name: &t.catalog_name,
            schema_name: &t.schema_name,
            table_name: &t.table_name,
        }
    }

    pub fn with_table_global_key(table_id: u64, t: &'a TableGlobalKey) -> Self {
        Self {
            table_id,
            catalog_name: &t.catalog_name,
            schema_name: &t.schema_name,
            table_name: &t.table_name,
        }
    }

    pub fn prefix(&self) -> String {
        format!(
            "{}-{}-{}-{}",
            TABLE_ROUTE_PREFIX, self.catalog_name, self.schema_name, self.table_name
        )
    }

    pub fn key(&self) -> String {
        format!("{}-{}", self.prefix(), self.table_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datanode_lease_key() {
        let key = LeaseKey {
            cluster_id: 0,
            node_id: 1,
        };

        let key_bytes: Vec<u8> = key.clone().try_into().unwrap();
        let new_key: LeaseKey = key_bytes.try_into().unwrap();

        assert_eq!(new_key, key);
    }

    #[test]
    fn test_datanode_lease_value() {
        let value = LeaseValue {
            timestamp_millis: 111,
            node_addr: "0.0.0.0:3002".to_string(),
        };

        let value_bytes: Vec<u8> = value.clone().try_into().unwrap();
        let new_value: LeaseValue = value_bytes.try_into().unwrap();

        assert_eq!(new_value, value);
    }
}
