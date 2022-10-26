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

pub(crate) const DATANODE_REGISTER_PREFIX: &str = "__dn";

lazy_static! {
    static ref DATANODE_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{}-([a-zA-Z_]+)-([a-zA-Z_]+)$",
        DATANODE_REGISTER_PREFIX
    ))
    .unwrap();
}

pub trait KvBytes: Sized {
    type Err;

    fn from_bytes(bytes: Vec<u8>) -> StdResult<Self, Self::Err>;

    fn into_bytes(self) -> StdResult<Vec<u8>, Self::Err>;
}

pub trait Value {
    fn into_value(self) -> Vec<u8>;
}

#[derive(Debug)]
pub struct DatanodeKey {
    pub addr: String,
    pub id: String,
}

impl FromStr for DatanodeKey {
    type Err = error::Error;

    fn from_str(key: &str) -> crate::Result<Self> {
        let captures = DATANODE_KEY_PATTERN
            .captures(key)
            .context(error::InvalidDatanodeKeySnafu { key })?;
        ensure!(captures.len() == 3, error::InvalidDatanodeKeySnafu { key });
        Ok(Self {
            addr: captures[1].to_string(),
            id: captures[2].to_string(),
        })
    }
}

impl KvBytes for DatanodeKey {
    type Err = error::Error;

    fn from_bytes(bytes: Vec<u8>) -> crate::Result<Self> {
        String::from_utf8(bytes)
            .context(error::DatanodeKeyFromUtf8Snafu {})
            .map(|x| x.parse())?
    }

    fn into_bytes(self) -> crate::Result<Vec<u8>> {
        Ok(format!("{}-{}-{}", DATANODE_REGISTER_PREFIX, self.addr, self.id).into_bytes())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatanodeValue {
    // last activity
    pub timestamp_millis: i64,
}

impl FromStr for DatanodeValue {
    type Err = error::Error;

    fn from_str(value: &str) -> crate::Result<Self> {
        serde_json::from_str(value).context(error::DeserializeDatanodeValueSnafu)
    }
}

impl KvBytes for DatanodeValue {
    type Err = error::Error;

    fn from_bytes(bytes: Vec<u8>) -> crate::Result<Self> {
        String::from_utf8(bytes)
            .context(error::DatanodeKeyFromUtf8Snafu {})
            .map(|x| x.parse())?
    }

    fn into_bytes(self) -> crate::Result<Vec<u8>> {
        Ok(serde_json::to_string(&self)
            .context(error::SerializeDatanodeValueSnafu)?
            .into_bytes())
    }
}
