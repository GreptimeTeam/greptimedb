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
pub mod meta;

use serde::{Deserialize, Serialize};

use crate::wal::kafka::KafkaOptions;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
pub enum WalProvider {
    #[default]
    RaftEngine,
    Kafka,
}

// Options of different wal providers are integrated into the WalOptions.
// There're no options related raft engine since it's a local wal.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct WalOptions {
    pub provider: WalProvider,
    pub kafka_opts: Option<KafkaOptions>,
}

// FIXME(niebayes): Is toml able to handle Option properly?
impl Default for WalOptions {
    fn default() -> Self {
        Self {
            provider: WalProvider::RaftEngine,
            kafka_opts: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toml_none_kafka_opts() {
        let opts = WalOptions::default();
        let toml_string = toml::to_string(&opts).unwrap();
        let _parsed: WalOptions = toml::from_str(&toml_string).unwrap();
    }

    #[test]
    fn test_toml_some_kafka_opts() {
        let opts = WalOptions {
            provider: WalProvider::Kafka,
            kafka_opts: Some(KafkaOptions::default()),
        };
        let toml_string = toml::to_string(&opts).unwrap();
        let _parsed: WalOptions = toml::from_str(&toml_string).unwrap();
    }
}
