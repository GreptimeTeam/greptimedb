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

use serde::{Deserialize, Deserializer, Serialize};
use snafu::OptionExt;
use uuid::Uuid;

use crate::error::{self, Result};

/// Persisted Authority Head format supported by this reader and writer.
pub const HEAD_FORMAT_VERSION: u32 = 1;

/// A process identity, generated once at startup and retained by the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub addr: String,
    pub incarnation_id: Uuid,
}

impl NodeIdentity {
    /// Creates a fresh process incarnation, even when the address is reused.
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            incarnation_id: Uuid::new_v4(),
        }
    }
}

/// Version-one persisted authority payload. Lease deadline is diagnostic only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityHead {
    pub format_version: u32,
    #[serde(deserialize_with = "required_owner")]
    pub owner: Option<NodeIdentity>,
    pub epoch: u64,
    pub lease_deadline_ms: u64,
    pub write_seq: u64,
}

fn required_owner<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<NodeIdentity>, D::Error> {
    Option::deserialize(deserializer)
}

impl AuthorityHead {
    /// Empty Head for explicit initialization of a new namespace.
    pub fn initial() -> Self {
        Self {
            format_version: HEAD_FORMAT_VERSION,
            owner: None,
            epoch: 0,
            lease_deadline_ms: 0,
            write_seq: 0,
        }
    }

    /// Validates the format and encodes compact deterministic JSON.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|_| {
            error::AuthorityFormatSnafu {
                reason: "JSON encoding failed",
            }
            .build()
        })
    }

    /// Decodes required fields and rejects unknown protocol versions.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let head: Self = serde_json::from_slice(bytes).map_err(|_| {
            error::AuthorityFormatSnafu {
                reason: "invalid JSON fields or types",
            }
            .build()
        })?;
        head.validate()?;
        Ok(head)
    }

    fn validate(&self) -> Result<()> {
        if self.format_version != HEAD_FORMAT_VERSION {
            return error::AuthorityVersionSnafu {
                version: self.format_version,
            }
            .fail();
        }
        Ok(())
    }

    /// Computes the next epoch without mutating or publishing this state.
    pub fn checked_next_epoch(&self) -> Result<u64> {
        self.epoch
            .checked_add(1)
            .context(error::AuthorityOverflowSnafu { counter: "epoch" })
    }

    /// Computes the next write sequence without publishing it.
    pub fn checked_next_write_seq(&self) -> Result<u64> {
        self.write_seq
            .checked_add(1)
            .context(error::AuthorityOverflowSnafu {
                counter: "write_seq",
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const INITIAL: &str = include_str!(
        "../../../../../../tests/compatibility/authority/object_store/initial_v1.json"
    );
    const OWNED: &str =
        include_str!("../../../../../../tests/compatibility/authority/object_store/owned_v1.json");

    #[test]
    fn test_head_v1_fixtures() {
        assert_eq!(
            AuthorityHead::decode(INITIAL.as_bytes()).unwrap(),
            AuthorityHead::initial()
        );
        for fixture in [INITIAL, OWNED] {
            let head = AuthorityHead::decode(fixture.as_bytes()).unwrap();
            assert_eq!(head.encode().unwrap(), fixture.trim_end().as_bytes());
        }
        let owner = AuthorityHead::decode(OWNED.as_bytes())
            .unwrap()
            .owner
            .unwrap();
        assert_eq!(owner.addr, "127.0.0.1:3002");
        assert_eq!(
            owner.incarnation_id.to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        let identity = NodeIdentity::new(owner.addr.clone());
        assert_ne!(NodeIdentity::new(owner.addr), identity);
    }

    #[test]
    fn test_head_v1_unknown_fields() {
        let mut value: serde_json::Value = serde_json::from_str(OWNED).unwrap();
        value["extra"] = true.into();
        value["owner"]["extra"] = 42.into();
        assert_eq!(
            AuthorityHead::decode(&serde_json::to_vec(&value).unwrap()).unwrap(),
            AuthorityHead::decode(OWNED.as_bytes()).unwrap()
        );
    }

    #[test]
    fn test_head_required_owner() {
        assert!(AuthorityHead::decode(INITIAL.as_bytes()).is_ok());
        let mut value: serde_json::Value = serde_json::from_str(INITIAL).unwrap();
        value.as_object_mut().unwrap().remove("owner");
        assert!(AuthorityHead::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn test_head_invalid_fields_and_version() {
        let value: serde_json::Value = serde_json::from_str(OWNED).unwrap();
        for field in [
            "format_version",
            "owner",
            "epoch",
            "lease_deadline_ms",
            "write_seq",
        ] {
            let mut missing = value.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(AuthorityHead::decode(&serde_json::to_vec(&missing).unwrap()).is_err());
            let mut wrong = value.clone();
            wrong[field] = true.into();
            assert!(AuthorityHead::decode(&serde_json::to_vec(&wrong).unwrap()).is_err());
        }
        for field in ["addr", "incarnation_id"] {
            let mut missing = value.clone();
            missing["owner"].as_object_mut().unwrap().remove(field);
            assert!(AuthorityHead::decode(&serde_json::to_vec(&missing).unwrap()).is_err());
        }
        let mut invalid = value.clone();
        invalid["owner"]["incarnation_id"] = "invalid".into();
        assert!(AuthorityHead::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
        invalid = value;
        invalid["format_version"] = 2.into();
        assert!(AuthorityHead::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
        let mut head = AuthorityHead::initial();
        head.format_version = 2;
        assert!(head.encode().is_err());
        assert!(AuthorityHead::decode(b"{").is_err());
    }

    #[test]
    fn test_head_counter_overflow() {
        let mut head = AuthorityHead::initial();
        assert_eq!(head.checked_next_epoch().unwrap(), 1);
        assert_eq!(head.checked_next_write_seq().unwrap(), 1);
        head.epoch = u64::MAX;
        head.write_seq = u64::MAX;
        assert!(head.checked_next_epoch().is_err());
        assert!(head.checked_next_write_seq().is_err());
    }
}
