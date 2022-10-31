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

pub(crate) const DN_LEASE_PREFIX: &str = "__meta_dnlease";

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

    fn from_str(value: &str) -> crate::Result<Self> {
        serde_json::from_str(value).context(error::DeserializeDatanodeValueSnafu)
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
            .context(error::SerializeDatanodeValueSnafu)?
            .into_bytes())
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
