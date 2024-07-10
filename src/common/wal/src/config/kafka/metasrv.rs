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

use serde::{Deserialize, Serialize};

use crate::config::kafka::common::{backoff_prefix, BackoffConfig, KafkaTopicConfig};
use crate::BROKER_ENDPOINT;

/// Kafka wal configurations for metasrv.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetasrvKafkaConfig {
    /// The broker endpoints of the Kafka cluster.
    pub broker_endpoints: Vec<String>,
    /// The backoff config.
    #[serde(flatten, with = "backoff_prefix")]
    pub backoff: BackoffConfig,
    /// The kafka config.
    #[serde(flatten)]
    pub kafka_topic: KafkaTopicConfig,
}

impl Default for MetasrvKafkaConfig {
    fn default() -> Self {
        let broker_endpoints = vec![BROKER_ENDPOINT.to_string()];
        Self {
            broker_endpoints,
            backoff: BackoffConfig::default(),
            kafka_topic: KafkaTopicConfig::default(),
        }
    }
}
