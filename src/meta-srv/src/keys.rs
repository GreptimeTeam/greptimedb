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
            err_msg: format!("invalid cluster_id: {}", cluster_id),
        })?;
        let node_id = node_id.parse::<u64>().context(error::ParseNumSnafu {
            err_msg: format!("invalid node_id: {}", node_id),
        })?;

        Ok(Self {
            cluster_id,
            node_id,
        })
    }
}

impl TryFrom<Vec<u8>> for DatanodeKey {
    type Error = error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(error::DatanodeKeyFromUtf8Snafu {})
            .map(|x| x.parse())?
    }
}

impl TryFrom<DatanodeKey> for Vec<u8> {
    type Error = error::Error;

    fn try_from(dn_key: DatanodeKey) -> Result<Self> {
        Ok(format!(
            "{}-{}-{}",
            DATANODE_REGISTER_PREFIX, dn_key.cluster_id, dn_key.node_id
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

impl TryFrom<Vec<u8>> for DatanodeValue {
    type Error = error::Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes)
            .context(error::DatanodeKeyFromUtf8Snafu {})
            .map(|x| x.parse())?
    }
}

impl TryFrom<DatanodeValue> for Vec<u8> {
    type Error = error::Error;

    fn try_from(dn_value: DatanodeValue) -> Result<Self> {
        Ok(serde_json::to_string(&dn_value)
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

        let key_bytes: Vec<u8> = key.clone().try_into().unwrap();
        let new_key: DatanodeKey = key_bytes.try_into().unwrap();

        assert_eq!(new_key, key);
    }

    #[test]
    fn test_datanode_value() {
        let value = DatanodeValue {
            timestamp_millis: 111,
            node_addr: "0.0.0.0:3002".to_string(),
        };

        let value_bytes: Vec<u8> = value.clone().try_into().unwrap();
        let new_value: DatanodeValue = value_bytes.try_into().unwrap();

        assert_eq!(new_value, value);
    }
}
