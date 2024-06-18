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

pub use datanode::*;
pub use flownode::*;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaseValue {
    // last activity
    pub timestamp_millis: i64,
    pub node_addr: String,
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

    fn try_from(dn_value: LeaseValue) -> crate::Result<Self> {
        Ok(serde_json::to_string(&dn_value)
            .context(error::SerializeToJsonSnafu {
                input: format!("{dn_value:?}"),
            })?
            .into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_value_round_trip() {
        let value = LeaseValue {
            timestamp_millis: 111,
            node_addr: "127.0.0.1:3002".to_string(),
        };

        let value_bytes: Vec<u8> = value.clone().try_into().unwrap();
        let new_value: LeaseValue = value_bytes.try_into().unwrap();

        assert_eq!(new_value, value);
    }
}
