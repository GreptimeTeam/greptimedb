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

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error;
use crate::key::{MetadataKey, PROCESS_LIST_PATTERN, PROCESS_LIST_PREFIX};

/// Key for running queries tracked in metasrv.
/// Layout: `__process/{frontend ip}-{query id}`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessKey {
    //todo(hl): maybe we don't have to own a string
    pub frontend_ip: String,
    pub id: u64,
}

impl Display for ProcessKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}-{}",
            PROCESS_LIST_PREFIX, self.frontend_ip, self.id
        )
    }
}

impl MetadataKey<'_, ProcessKey> for ProcessKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> crate::error::Result<ProcessKey> {
        let key_str = std::str::from_utf8(bytes).map_err(|_e| {
            error::InvalidProcessKeySnafu {
                key: String::from_utf8_lossy(bytes).into_owned(),
            }
            .build()
        })?;
        let captures = PROCESS_LIST_PATTERN.captures(key_str).with_context(|| {
            error::InvalidProcessKeySnafu {
                key: String::from_utf8_lossy(bytes).into_owned(),
            }
        })?;
        let id = u64::from_str(&captures[2]).map_err(|_| {
            error::InvalidProcessKeySnafu {
                key: String::from_utf8_lossy(bytes).into_owned(),
            }
            .build()
        })?;
        Ok(Self {
            frontend_ip: captures[1].to_string(),
            id,
        })
    }
}

/// Detail value of process.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessValue {
    /// The running query sql.
    pub query: String,
    /// Query start timestamp in milliseconds.
    pub start_timestamp_ms: i64,
}

#[cfg(test)]
mod tests {
    use crate::key::process_list::ProcessKey;
    use crate::key::MetadataKey;

    fn check_serialization(input: ProcessKey) {
        let serialized = input.to_bytes();
        let key = ProcessKey::from_bytes(&serialized).unwrap();
        assert_eq!(&input.frontend_ip, &key.frontend_ip);
        assert_eq!(input.id, key.id);
    }

    #[test]
    fn test_capture_process_list() {
        check_serialization(ProcessKey {
            frontend_ip: "192.168.0.1:4001".to_string(),
            id: 1,
        });
        check_serialization(ProcessKey {
            frontend_ip: "192.168.0.1:4002".to_string(),
            id: 0,
        });
        check_serialization(ProcessKey {
            frontend_ip: "255.255.255.255:80".to_string(),
            id: 0,
        });
    }
}
