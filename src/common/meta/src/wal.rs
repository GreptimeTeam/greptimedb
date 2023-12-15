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

pub mod kafka;
pub mod options_allocator;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use store_api::storage::RegionNumber;

use crate::error::Result;
use crate::wal::kafka::{KafkaConfig, KafkaWalOptions};
pub use crate::wal::options_allocator::WalOptionsAllocator;

/// Wal configurations for bootstraping meta srv.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "provider")]
pub enum WalConfig {
    #[default]
    #[serde(rename = "raft-engine")]
    RaftEngine,
    #[serde(rename = "kafka")]
    Kafka(KafkaConfig),
}

/// Wal options for a region.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub enum WalOptions {
    #[default]
    RaftEngine,
    Kafka(KafkaWalOptions),
}

// TODO(niebayes): determine how to encode wal options.
pub type EncodedWalOptions = HashMap<String, String>;

impl From<WalOptions> for EncodedWalOptions {
    fn from(value: WalOptions) -> Self {
        // TODO(niebayes): implement encoding/decoding for wal options.
        EncodedWalOptions::default()
    }
}

impl TryFrom<EncodedWalOptions> for WalOptions {
    type Error = crate::error::Error;

    fn try_from(value: EncodedWalOptions) -> Result<Self> {
        todo!()
    }
}
