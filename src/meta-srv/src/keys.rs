use std::result::Result as StdResult;
use std::str::FromStr;

use lazy_static::lazy_static;
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;
use snafu::ensure;
use snafu::OptionExt;
use snafu::ResultExt;

use crate::error;
use crate::error::Result;

pub(crate) const DATANODE_REGISTER_PREFIX: &str = "__dn";

lazy_static! {
    static ref DATANODE_KEY_PATTERN: Regex =
        Regex::new(&format!("^{}-([0-9]+)-([0-9]+)$", DATANODE_REGISTER_PREFIX)).unwrap();
}

pub trait KvBytes: Sized {
    type Err;

    fn from_bytes(bytes: Vec<u8>) -> StdResult<Self, Self::Err>;

    fn into_bytes(self) -> StdResult<Vec<u8>, Self::Err>;
}

pub trait Value {
    fn into_value(self) -> Vec<u8>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DatanodeKey {
    pub cluster_id: u64,
    pub node_id: u64,
}

impl FromStr for DatanodeKey {
    type Err = error::Error;

    fn from_str(key: &str) -> Result<Self> {
        let caps = DATANODE_KEY_PATTERN
            .captures(key)
            .context(error::InvalidDatanodeKeySnafu { key })?;
        ensure!(caps.len() == 3, error::InvalidDatanodeKeySnafu { key });
        let cluster_id = caps[1].to_string();
        let node_id = caps[2].to_string();
        let cluster_id = cluster_id.parse::<u64>().context(error::ParseNumSnafu {
            err_msg: format!("cluster_id: {}", cluster_id),
        })?;
        let node_id = node_id.parse::<u64>().context(error::ParseNumSnafu {
            err_msg: format!("node_id: {}", node_id),
        })?;
        Ok(Self {
            cluster_id,
            node_id,
        })
    }
}

impl KvBytes for DatanodeKey {
    type Err = error::Error;

    fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(error::DatanodeKeyFromUtf8Snafu {})
            .map(|x| x.parse())?
    }

    fn into_bytes(self) -> Result<Vec<u8>> {
        Ok(format!(
            "{}-{}-{}",
            DATANODE_REGISTER_PREFIX, self.cluster_id, self.node_id
        )
        .into_bytes())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatanodeValue {
    // last activity
    pub timestamp_millis: i64,
    pub node_addr: String,
}

impl FromStr for DatanodeValue {
    type Err = error::Error;

    fn from_str(value: &str) -> crate::Result<Self> {
        serde_json::from_str(value).context(error::DeserializeDatanodeValueSnafu)
    }
}

impl KvBytes for DatanodeValue {
    type Err = error::Error;

    fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(error::DatanodeKeyFromUtf8Snafu {})
            .map(|x| x.parse())?
    }

    fn into_bytes(self) -> Result<Vec<u8>> {
        Ok(serde_json::to_string(&self)
            .context(error::SerializeDatanodeValueSnafu)?
            .into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datanode_key() {
        let key = DatanodeKey {
            cluster_id: 0,
            node_id: 1,
        };

        let key_bytes = key.clone().into_bytes().unwrap();
        let new_key = DatanodeKey::from_bytes(key_bytes).unwrap();

        assert_eq!(new_key, key);
    }

    #[test]
    fn test_datanode_value() {
        let value = DatanodeValue {
            timestamp_millis: 111,
            node_addr: "0.0.0.0:3002".to_string(),
        };

        let value_bytes = value.clone().into_bytes().unwrap();
        let new_value = DatanodeValue::from_bytes(value_bytes).unwrap();

        assert_eq!(new_value, value);
    }
}
