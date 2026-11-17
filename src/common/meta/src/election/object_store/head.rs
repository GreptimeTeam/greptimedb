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
use snafu::{OptionExt, ResultExt};
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
    // Serde normally treats a missing Option as None. The design requires owner to
    // be present while still allowing its value to be null.
    #[serde(deserialize_with = "required_owner")]
    pub owner: Option<NodeIdentity>,
    epoch: u64,
    pub lease_deadline_ms: u64,
    write_seq: u64,
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
        serde_json::to_vec(self).context(error::AuthorityFormatSnafu {
            operation: "encode",
        })
    }

    /// Decodes required fields and rejects unknown protocol versions.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let head: Self = serde_json::from_slice(bytes).context(error::AuthorityFormatSnafu {
            operation: "decode",
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

    /// Returns the logical epoch in this payload.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Returns the write sequence in this payload.
    pub fn write_seq(&self) -> u64 {
        self.write_seq
    }

    /// Returns a payload with its epoch advanced, rejecting overflow before mutation.
    pub fn with_next_epoch(mut self) -> Result<Self> {
        self.epoch = self
            .epoch
            .checked_add(1)
            .context(error::AuthorityOverflowSnafu { counter: "epoch" })?;
        Ok(self)
    }

    /// Returns a payload with its write sequence advanced, rejecting overflow.
    pub fn with_next_write_seq(mut self) -> Result<Self> {
        self.write_seq = self
            .write_seq
            .checked_add(1)
            .context(error::AuthorityOverflowSnafu {
                counter: "write_seq",
            })?;
        Ok(self)
    }
}
