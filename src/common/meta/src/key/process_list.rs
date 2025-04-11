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
use std::time::Duration;

use common_time::util::current_time_millis;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error;
use crate::key::{MetadataKey, PROCESS_LIST_PATTERN, PROCESS_LIST_PREFIX};

/// Key for running queries tracked in metasrv.
/// Layout: `__process/{frontend server addr}-{query id}`
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

    fn from_bytes(bytes: &[u8]) -> error::Result<ProcessKey> {
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
    /// Database name.
    pub database: String,
    /// The running query sql.
    pub query: String,
    /// Query start timestamp in milliseconds.
    pub start_timestamp_ms: i64,
}

/// Sequence key for process list entries.
pub const PROCESS_ID_SEQ: &str = "process_id_seq";

/// Running process instance.
pub struct Process {
    pub key: ProcessKey,
    pub value: ProcessValue,
}

impl Process {
    /// Returns server address of running process.
    pub fn server_addr(&self) -> &str {
        &self.key.frontend_ip
    }

    /// Returns database name of running process.
    pub fn database(&self) -> &str {
        &self.value.database
    }

    /// Returns id of query.
    pub fn query_id(&self) -> u64 {
        self.key.id
    }

    /// Returns query string details.
    pub fn query_string(&self) -> &str {
        &self.value.query
    }

    /// Returns query start timestamp in milliseconds.
    pub fn query_start_timestamp_ms(&self) -> i64 {
        self.value.start_timestamp_ms
    }

    /// Calculates the elapsed time of query. Returns None of system clock jumps backwards.
    pub fn query_elapsed(&self) -> Option<Duration> {
        let now = current_time_millis();
        if now < self.value.start_timestamp_ms {
            None
        } else {
            Some(Duration::from_millis(
                (now - self.value.start_timestamp_ms) as u64,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn test_process_key_display() {
        let key = ProcessKey {
            frontend_ip: "127.0.0.1:3000".to_string(),
            id: 42,
        };
        assert_eq!(key.to_string(), "__process/127.0.0.1:3000-42");
    }

    #[test]
    fn test_process_key_from_bytes_valid() {
        let bytes = b"__process/10.0.0.1:8080-123";
        let key = ProcessKey::from_bytes(bytes).unwrap();
        assert_eq!(key.frontend_ip, "10.0.0.1:8080");
        assert_eq!(key.id, 123);
    }

    #[test]
    fn test_process_key_from_bytes_invalid_format() {
        let bytes = b"invalid_format";
        let result = ProcessKey::from_bytes(bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_key_from_bytes_invalid_id() {
        let bytes = b"__process/10.0.0.1:8080-abc";
        let result = ProcessKey::from_bytes(bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_value_serialization() {
        let value = ProcessValue {
            query: "SELECT * FROM test".to_string(),
            start_timestamp_ms: 1690000000000,
            database: "public".to_string(),
        };

        // Test serialization roundtrip
        let serialized = serde_json::to_string(&value).unwrap();
        let deserialized: ProcessValue = serde_json::from_str(&serialized).unwrap();

        assert_eq!(value.query, deserialized.query);
        assert_eq!(value.start_timestamp_ms, deserialized.start_timestamp_ms);
        assert_eq!(value.database, deserialized.database);
    }
}
